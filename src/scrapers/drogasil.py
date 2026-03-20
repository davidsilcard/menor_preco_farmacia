import asyncio
import json
import random
import re
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.core.config import settings
from src.models.base import Pharmacy, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class DrogasilScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://www.drogasil.com.br")
        self.base_domain = "https://www.drogasil.com.br"
        self.search_terms = [term.strip() for term in settings.DROGASIL_SEARCH_TERMS.split(",") if term.strip()]
        self.max_products_per_term = 12
        self.detail_concurrency = 3

    async def scrape(self):
        async with async_playwright() as p:
            browser, context = await self.get_browser_context(p)
            page = await context.new_page()

            all_products = []
            seen_urls = set()

            for term in self.search_terms:
                search_url = f"{self.base_domain}/search?w={quote_plus(term)}"
                print(f"Buscando na Drogasil: {search_url}")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2, 3))
                await self._configure_cep(page)

                listing_products = await self._collect_listing_products(page, term)
                for product in listing_products:
                    if product["source_url"] in seen_urls:
                        continue
                    seen_urls.add(product["source_url"])
                    all_products.append(product)

            semaphore = asyncio.Semaphore(self.detail_concurrency)
            tasks = [self._enrich_listing_product(context, semaphore, product) for product in all_products]
            enriched_products = [product for product in await asyncio.gather(*tasks) if product]

            await browser.close()
            return enriched_products

    async def _configure_cep(self, page):
        try:
            cep_trigger = page.locator("text='Insira seu CEP'").first
            if await cep_trigger.is_visible(timeout=3000):
                await cep_trigger.click()
                await asyncio.sleep(1)
                cep_input = page.locator("#cep")
                await cep_input.fill(self.cep)
                await asyncio.sleep(0.5)
                await cep_input.press("Enter")
                await asyncio.sleep(random.uniform(2, 3))
                print(f"CEP {self.cep} configurado na Drogasil.")
        except Exception as e:
            print(f"Aviso: fluxo de CEP da Drogasil pulado ou falhou: {e}")

    async def _collect_listing_products(self, page, term):
        await asyncio.sleep(2)
        hrefs = await page.locator("a[href*='.html']").evaluate_all(
            """els => [...new Set(
                els.map(e => e.href)
                   .filter(h => h.includes('.html') && !h.includes('/medicamentos.html') && !h.includes('/saude.html'))
            )]"""
        )

        products = []
        for href in hrefs:
            if "origin=search" not in href:
                continue
            products.append(
                {
                    "source_url": href,
                    "search_term": term,
                    "availability": "unknown",
                    "source_metadata": {
                        "search_term": term,
                        "search_url": page.url,
                    },
                }
            )
            if len(products) >= self.max_products_per_term:
                break

        print(f"Drogasil: {len(products)} produtos coletados para o termo '{term}'.")
        return products

    async def _enrich_listing_product(self, context, semaphore, product):
        async with semaphore:
            detail_page = await context.new_page()
            try:
                await asyncio.sleep(random.uniform(0.5, 1.2))
                await detail_page.goto(product["source_url"], wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2, 3))
                html = await detail_page.content()
                enriched = self._extract_product_detail(html, product)
                print(
                    "Drogasil detalhe:",
                    enriched["source_sku"],
                    enriched.get("ean_gtin") or "sem-ean",
                    enriched.get("anvisa_code") or "sem-anvisa",
                )
                return enriched
            except Exception as e:
                print(f"Aviso: falha ao enriquecer produto da Drogasil {product['source_url']}: {e}")
                return None
            finally:
                await detail_page.close()

    def _extract_product_detail(self, html, product):
        soup = BeautifulSoup(html, "html.parser")
        metadata = dict(product.get("source_metadata") or {})
        metadata["detail_extractors"] = []

        product_schema = self._extract_product_schema(soup)
        if product_schema:
            metadata["detail_extractors"].append("json_ld")

        raw_name = self._first_non_empty(
            product_schema.get("name") if product_schema else None,
            soup.find("meta", attrs={"property": "og:title"}).get("content") if soup.find("meta", attrs={"property": "og:title"}) else None,
        )
        normalized_name = self.normalize_text(raw_name)
        structured_fields = self.extract_structured_fields(raw_name)

        body_text = soup.get_text(" ", strip=True)
        normalized_body = self.normalize_text(body_text)

        source_sku = self._extract_source_sku(product_schema, normalized_body)
        price = self._extract_price(product_schema, normalized_body)
        ean_gtin = self._extract_ean(product_schema, normalized_body)
        anvisa_code = self._extract_anvisa_code(normalized_body)
        brand = self._extract_brand(product_schema, normalized_body)
        manufacturer = self._extract_labeled_value(normalized_body, "fabricante")
        active_ingredient = self._extract_labeled_value(normalized_body, "principio ativo")
        dosage = self._extract_labeled_value(normalized_body, "dosagem") or structured_fields.get("dosage")
        pack_size = self._extract_labeled_value(normalized_body, "quantidade") or structured_fields.get("pack_size")
        promotion_text = self._extract_promotion_text(normalized_body)
        availability = self.availability_from_schema(product_schema, normalized_body)

        metadata["json_ld"] = product_schema

        return {
            **product,
            "raw_name": raw_name,
            "normalized_name": normalized_name,
            "source_sku": source_sku,
            "price": price,
            "brand": brand,
            "manufacturer": manufacturer,
            "active_ingredient": active_ingredient,
            "dosage": dosage,
            "presentation": structured_fields.get("presentation"),
            "pack_size": pack_size,
            "ean_gtin": ean_gtin,
            "anvisa_code": anvisa_code,
            "promotion_text": promotion_text,
            "availability": availability,
            "source_metadata": metadata,
        }

    def _extract_product_schema(self, soup):
        for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
            raw_json = script.string or script.get_text(strip=True)
            if not raw_json:
                continue
            try:
                payload = json.loads(raw_json)
            except json.JSONDecodeError:
                continue

            candidates = payload if isinstance(payload, list) else [payload]
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("@type") == "Product":
                    return candidate
        return None

    def _extract_source_sku(self, product_schema, normalized_body):
        if product_schema and product_schema.get("sku"):
            return str(product_schema["sku"])
        match = re.search(r"codigo do produto\D{0,10}(\d+)", normalized_body)
        return match.group(1) if match else None

    def _extract_price(self, product_schema, normalized_body):
        if product_schema:
            offers = product_schema.get("offers") or {}
            price = offers.get("price")
            if price is not None:
                return float(str(price).replace(",", "."))

        match = re.search(r"r\$\s*(\d+[.,]\d{2})", normalized_body)
        if match:
            return float(match.group(1).replace(",", "."))
        raise ValueError("Preco nao encontrado na pagina da Drogasil.")

    def _extract_ean(self, product_schema, normalized_body):
        if product_schema:
            for key in ("gtin13", "gtin", "gtin14"):
                value = product_schema.get(key)
                if isinstance(value, str):
                    digits = re.sub(r"\D", "", value)
                    if len(digits) in (8, 12, 13, 14):
                        return digits
        match = re.search(r"(?:ean|codigo de barras|gtin)\D{0,10}(\d{8,14})", normalized_body)
        return match.group(1) if match else None

    def _extract_anvisa_code(self, normalized_body):
        match = re.search(r"(?:registro ms|registro anvisa|anvisa|ms)\D{0,10}(\d{8,13})", normalized_body)
        return match.group(1) if match else None

    def _extract_brand(self, product_schema, normalized_body):
        if product_schema:
            brand = product_schema.get("brand")
            if isinstance(brand, dict):
                return brand.get("name")
            if isinstance(brand, str):
                return brand
        return self._extract_labeled_value(normalized_body, "marca")

    def _extract_labeled_value(self, normalized_body, label):
        match = re.search(rf"{label}\s+([a-z0-9\s\-/,'.]+?)(?=\s+(?:marca|quantidade|principio ativo|caracteristicas|codigo do produto|fabricante|registro ms|dosagem|quem comprou|$))", normalized_body)
        if match:
            return match.group(1).strip()
        return None

    def _extract_promotion_text(self, normalized_body):
        promo_match = re.search(r"(leve\s+\d+.*?pague\s+\d+|desconto\s+de\s+\d+%?)", normalized_body)
        return promo_match.group(1) if promo_match else None

    def save_to_db(self, products):
        if not products:
            print("Nenhum produto da Drogasil encontrado para salvar.")
            return

        session = SessionLocal()
        try:
            pharmacy = session.query(Pharmacy).filter_by(slug="drogasil").first()
            if not pharmacy:
                raise ValueError("Farmacia Drogasil nao cadastrada. Rode src.init_db primeiro.")
            matcher = ProductMatcher(session)

            for product_data in products:
                if not product_data.get("source_sku") or not product_data.get("price"):
                    continue

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
            print(f"Sucesso: {len(products)} snapshots de precos da Drogasil salvos no banco.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar produtos da Drogasil no banco: {e}")
        finally:
            session.close()

    @staticmethod
    def _first_non_empty(*values):
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None


if __name__ == "__main__":
    scraper = DrogasilScraper()
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scraper.scrape())
    if data:
        scraper.save_to_db(data)
