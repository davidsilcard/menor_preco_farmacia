import asyncio
import json
import random
import re
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.core.config import settings
from src.models.base import (
    Pharmacy,
    PriceSnapshot,
    ProductMatch,
    SessionLocal,
    SourceProduct,
)
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class PanvelScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://www.panvel.com/panvel/buscarProduto.do?termoPesquisa=dipirona")
        self.base_domain = "https://www.panvel.com"
        self.detail_concurrency = 3
        self.search_terms = [term.strip() for term in settings.PANVEL_SEARCH_TERMS.split(",") if term.strip()]
        self.max_products_per_term = 16

    async def scrape(self):
        async with async_playwright() as p:
            browser, context = await self.get_browser_context(p)
            page = await context.new_page()

            listing_products = []
            seen_skus = set()

            for index, term in enumerate(self.search_terms):
                search_url = f"{self.base_domain}/panvel/buscarProduto.do?termoPesquisa={quote_plus(term)}"
                print(f"Buscando na Panvel: {search_url}")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2, 4))

                if index == 0:
                    await self._configure_cep(page)

                term_products = await self._collect_listing_products(page, term)
                for product in term_products:
                    sku = product["source_sku"]
                    if sku in seen_skus:
                        continue
                    seen_skus.add(sku)
                    listing_products.append(product)

            semaphore = asyncio.Semaphore(self.detail_concurrency)
            tasks = [self._enrich_listing_product(context, semaphore, product) for product in listing_products]
            enriched_products = [product for product in await asyncio.gather(*tasks) if product]

            await browser.close()
            return enriched_products

    async def _configure_cep(self, page):
        try:
            cep_button = page.get_by_role("button", name="Informe seu CEP")
            if await cep_button.is_visible():
                await cep_button.click()
                await asyncio.sleep(random.uniform(1, 2))

                input_cep = page.locator('input[placeholder*="00000-000"]')
                await input_cep.fill(self.cep)
                await asyncio.sleep(1)
                await input_cep.press("Enter")

                print(f"CEP {self.cep} configurado com sucesso.")
                await asyncio.sleep(random.uniform(3, 5))
        except Exception as e:
            print(f"Aviso: Fluxo de CEP pulado ou falhou: {e}")

    async def _collect_listing_products(self, page, term):
        products_found = []

        await page.wait_for_selector('a[href*="/p-"]', timeout=15000)
        product_links = await page.query_selector_all('a[href*="/p-"]')

        seen_skus = set()
        print(f"Encontrados {len(product_links)} possiveis itens na pagina.")

        for link in product_links:
            try:
                text_content = await link.inner_text()
                href = await link.get_attribute("href")

                if not href:
                    continue

                sku_match = re.search(r"p-(\d+)", href)
                source_sku = sku_match.group(1) if sku_match else None

                if not source_sku or source_sku in seen_skus:
                    continue
                seen_skus.add(source_sku)

                price_match = re.search(r"R\$\s?(\d+,\d{2})", text_content)
                if not price_match:
                    continue

                raw_name = text_content.split("\n")[0].strip()
                structured_fields = self.extract_structured_fields(raw_name)
                full_url = href if href.startswith("http") else f"{self.base_domain}{href}"

                products_found.append(
                    {
                        "raw_name": raw_name,
                        "source_sku": source_sku,
                        "price": float(price_match.group(1).replace(",", ".")),
                        "source_url": full_url,
                        "availability": "available",
                        "promotion_text": None,
                        "source_metadata": {
                            "search_term": term,
                            "listing_text": text_content.strip(),
                            "search_url": page.url,
                            "listing_url": full_url,
                        },
                        **structured_fields,
                    }
                )
            except Exception:
                continue

        return products_found[: self.max_products_per_term]

    async def _enrich_listing_product(self, context, semaphore, product):
        async with semaphore:
            detail_page = await context.new_page()
            try:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                await detail_page.goto(product["source_url"], wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(1, 2))
                html = await detail_page.content()
                enriched = self._extract_product_detail(html, product)
                if not self._matches_search_term(enriched):
                    return None
                print(
                    "Detalhe coletado:",
                    enriched["source_sku"],
                    enriched.get("ean_gtin") or "sem-ean",
                    enriched.get("anvisa_code") or "sem-anvisa",
                )
                return enriched
            except Exception as e:
                print(f"Aviso: falha ao enriquecer {product['source_url']}: {e}")
                return product
            finally:
                await detail_page.close()

    def _extract_product_detail(self, html, product):
        soup = BeautifulSoup(html, "html.parser")
        metadata = dict(product.get("source_metadata") or {})
        metadata["detail_extractors"] = []

        json_ld_data = self._extract_json_ld(soup)
        product_schema = self._select_product_schema(json_ld_data)
        if product_schema:
            metadata["detail_extractors"].append("json_ld")

        raw_name = (
            self._first_non_empty(
                product_schema.get("name") if product_schema else None,
                soup.find("meta", attrs={"property": "og:title"}).get("content") if soup.find("meta", attrs={"property": "og:title"}) else None,
                product["raw_name"],
            )
        )
        normalized_name = self.normalize_text(raw_name)
        structured_fields = self.extract_structured_fields(raw_name)

        body_text = soup.get_text(" ", strip=True)
        normalized_body = self.normalize_text(body_text)

        brand = self._extract_brand(product_schema, soup)
        manufacturer = self._extract_manufacturer(product_schema, soup) or brand
        active_ingredient = self._extract_active_ingredient(normalized_body)
        ean_gtin = self._extract_ean(product_schema, normalized_body)
        anvisa_code = self._extract_anvisa_code(normalized_body)
        promotion_text = self._extract_promotion_text(normalized_body)
        detail_dosage = self._extract_labeled_value(soup, "dosagem")
        detail_pack_size = self._extract_labeled_value(soup, "quantidade")

        metadata["json_ld"] = product_schema
        metadata["detail_title"] = raw_name

        return {
            **product,
            "raw_name": raw_name,
            "normalized_name": normalized_name,
            "brand": brand,
            "manufacturer": manufacturer,
            "active_ingredient": active_ingredient,
            "dosage": detail_dosage or structured_fields.get("dosage") or product.get("dosage"),
            "presentation": structured_fields.get("presentation") or product.get("presentation"),
            "pack_size": detail_pack_size or structured_fields.get("pack_size") or product.get("pack_size"),
            "ean_gtin": ean_gtin,
            "anvisa_code": anvisa_code,
            "promotion_text": promotion_text,
            "source_metadata": metadata,
        }

    def _extract_json_ld(self, soup):
        payloads = []
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw_json = script.string or script.get_text(strip=True)
            if not raw_json:
                continue
            try:
                payloads.append(json.loads(raw_json))
            except json.JSONDecodeError:
                continue
        return payloads

    def _select_product_schema(self, json_ld_data):
        for payload in json_ld_data:
            candidates = payload if isinstance(payload, list) else [payload]
            for candidate in candidates:
                if not isinstance(candidate, dict):
                    continue
                schema_type = candidate.get("@type")
                if schema_type == "Product" or (isinstance(schema_type, list) and "Product" in schema_type):
                    return candidate
                graph = candidate.get("@graph")
                if isinstance(graph, list):
                    for item in graph:
                        item_type = item.get("@type")
                        if item_type == "Product" or (isinstance(item_type, list) and "Product" in item_type):
                            return item
        return None

    def _extract_brand(self, product_schema, soup):
        if product_schema:
            brand = product_schema.get("brand")
            if isinstance(brand, dict):
                return brand.get("name")
            if isinstance(brand, str):
                return brand

        return self._extract_labeled_value(soup, "marca")

    def _extract_manufacturer(self, product_schema, soup):
        if product_schema:
            manufacturer = product_schema.get("manufacturer")
            if isinstance(manufacturer, dict):
                return manufacturer.get("name")
            if isinstance(manufacturer, str):
                return manufacturer

        return self._extract_labeled_value(soup, "fabricante|laboratorio")

    def _extract_ean(self, product_schema, normalized_body):
        if product_schema:
            for key in ("gtin13", "gtin", "gtin14"):
                digits = self.clean_identifier(product_schema.get(key))
                if digits:
                    return digits

        match = re.search(r"\b(?:ean|codigo de barras|gtin)\D{0,10}(\d{8,14})\b", normalized_body)
        return self.clean_identifier(match.group(1)) if match else None

    def _extract_anvisa_code(self, normalized_body):
        match = re.search(r"\b(?:registro anvisa|registro ms|anvisa|ms)\D{0,20}(\d{8,13})\b", normalized_body)
        return match.group(1) if match else None

    def _extract_active_ingredient(self, normalized_body):
        match = re.search(r"(principio ativo|substancia ativa)\s*[:\-]?\s*([a-z0-9\s,]+)", normalized_body)
        if match:
            return match.group(2).strip()
        return None

    def _extract_promotion_text(self, normalized_body):
        promo_match = re.search(r"(leve\s+\d+.*?pague\s+\d+|desconto\s+de\s+\d+%?)", normalized_body)
        return promo_match.group(1) if promo_match else None

    def _extract_labeled_value(self, soup, label_pattern):
        label = soup.find(string=re.compile(rf"^\s*({label_pattern})\s*:?\s*$", re.IGNORECASE))
        if not label:
            return None

        parent_text = label.parent.get_text(" ", strip=True) if label.parent else ""
        if parent_text:
            cleaned = re.sub(rf"^\s*({label_pattern})\s*:?\s*", "", parent_text, flags=re.IGNORECASE).strip()
            if 0 < len(cleaned) <= 120:
                return cleaned

        sibling = label.parent.find_next_sibling() if label.parent else None
        if sibling:
            sibling_text = sibling.get_text(" ", strip=True)
            if 0 < len(sibling_text) <= 120:
                return sibling_text

        return None

    def _matches_search_term(self, product_data):
        search_term = ((product_data.get("source_metadata") or {}).get("search_term") or "").strip()
        normalized_name = product_data.get("normalized_name") or ""
        if not search_term or not normalized_name:
            return True

        term_tokens = [token for token in self.normalize_text(search_term).split() if len(token) >= 4]
        if not term_tokens:
            return True

        return any(token in normalized_name for token in term_tokens)

    def save_to_db(self, products):
        if not products:
            print("Nenhum produto encontrado para salvar.")
            return

        session = SessionLocal()
        try:
            pharmacy = session.query(Pharmacy).filter_by(slug="panvel").first()
            if not pharmacy:
                raise ValueError("Farmacia Panvel nao cadastrada. Rode src.init_db primeiro.")
            matcher = ProductMatcher(session)

            for product_data in products:
                source_product = (
                    session.query(SourceProduct)
                    .filter_by(pharmacy_id=pharmacy.id, source_sku=product_data["source_sku"])
                    .first()
                )

                if not source_product:
                    source_product = SourceProduct(
                        pharmacy_id=pharmacy.id,
                        source_sku=product_data["source_sku"],
                        source_url=product_data.get("source_url"),
                        raw_name=product_data["raw_name"],
                        normalized_name=product_data["normalized_name"],
                        brand=product_data.get("brand"),
                        manufacturer=product_data.get("manufacturer"),
                        active_ingredient=product_data.get("active_ingredient"),
                        dosage=product_data.get("dosage"),
                        presentation=product_data.get("presentation"),
                        pack_size=product_data.get("pack_size"),
                        ean_gtin=product_data.get("ean_gtin"),
                        anvisa_code=product_data.get("anvisa_code"),
                        source_metadata=product_data.get("source_metadata"),
                    )
                    session.add(source_product)
                    session.flush()
                else:
                    source_product.source_url = product_data.get("source_url")
                    source_product.raw_name = product_data["raw_name"]
                    source_product.normalized_name = product_data["normalized_name"]
                    source_product.brand = product_data.get("brand")
                    source_product.manufacturer = product_data.get("manufacturer")
                    source_product.active_ingredient = product_data.get("active_ingredient")
                    source_product.dosage = product_data.get("dosage")
                    source_product.presentation = product_data.get("presentation")
                    source_product.pack_size = product_data.get("pack_size")
                    source_product.ean_gtin = product_data.get("ean_gtin")
                    source_product.anvisa_code = product_data.get("anvisa_code")
                    source_product.source_metadata = product_data.get("source_metadata")

                decision = matcher.match_source_product(product_data)
                canonical_product = decision.canonical_product or matcher.build_canonical_product(product_data)
                decision = matcher.resolve_match_metadata(canonical_product, product_data)
                if not source_product.match:
                    session.add(
                        ProductMatch(
                            source_product_id=source_product.id,
                            canonical_product_id=canonical_product.id,
                            match_type=decision.match_type,
                            confidence=decision.confidence,
                            review_status=decision.review_status,
                            review_notes=decision.review_notes,
                        )
                    )
                else:
                    source_product.match.canonical_product_id = canonical_product.id
                    source_product.match.match_type = decision.match_type
                    source_product.match.confidence = decision.confidence
                    source_product.match.review_status = decision.review_status
                    source_product.match.review_notes = decision.review_notes

                matcher.reconcile_canonical_matches(canonical_product)

                session.add(
                    PriceSnapshot(
                        source_product_id=source_product.id,
                        price=product_data["price"],
                        cep=self.cep,
                        availability=product_data.get("availability", "unknown"),
                        source_url=product_data.get("source_url"),
                        promotion_text=product_data.get("promotion_text"),
                    )
                )

            session.commit()
            print(f"Sucesso: {len(products)} snapshots de precos da Panvel salvos no banco.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar no banco: {e}")
        finally:
            session.close()

    @staticmethod
    def _first_non_empty(*values):
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None


if __name__ == "__main__":
    scraper = PanvelScraper()
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scraper.scrape())
    if data:
        scraper.save_to_db(data)
