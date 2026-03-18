import asyncio
import random
import re
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.core.config import settings
from src.models.base import Pharmacy, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class FarmaciaJaraguaScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://farmaciajaragua.com.br")
        self.base_domain = "https://farmaciajaragua.com.br"
        self.search_terms = [
            term.strip() for term in settings.FARMACIA_JARAGUA_SEARCH_TERMS.split(",") if term.strip()
        ]
        self.max_products_per_term = 12
        self.detail_concurrency = 3

    async def scrape(self):
        async with async_playwright() as p:
            browser, context = await self.get_browser_context(p)
            page = await context.new_page()

            all_products = []
            seen_urls = set()

            for term in self.search_terms:
                search_url = f"{self.base_domain}/?q={quote_plus(term)}"
                print(f"Buscando na Farmacia Jaragua: {search_url}")
                await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(3, 4))

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

    async def _collect_listing_products(self, page, term):
        hrefs = await page.locator("a[href]").evaluate_all(
            """els => [...new Set(
                els.map(e => e.href)
                   .filter(h => h.includes('farmaciajaragua.com.br/') && h.includes('/dXCXRx1xPa'))
            )]"""
        )

        products = []
        for href in hrefs:
            normalized_href = href.split("?")[0]
            products.append(
                {
                    "source_url": normalized_href,
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

        print(f"Farmacia Jaragua: {len(products)} produtos coletados para o termo '{term}'.")
        return products

    async def _enrich_listing_product(self, context, semaphore, product):
        async with semaphore:
            detail_page = await context.new_page()
            try:
                await asyncio.sleep(random.uniform(0.5, 1.2))
                await detail_page.goto(product["source_url"], wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(random.uniform(3, 4))
                body_text = await detail_page.text_content("body")
                html = await detail_page.content()
                enriched = self._extract_product_detail(html, body_text or "", product)
                if not self._matches_search_term(enriched):
                    return None
                print(
                    "Farmacia Jaragua detalhe:",
                    enriched["source_sku"],
                    enriched.get("ean_gtin") or "sem-ean",
                    enriched.get("anvisa_code") or "sem-anvisa",
                )
                return enriched
            except Exception as e:
                print(f"Aviso: falha ao enriquecer produto da Farmacia Jaragua {product['source_url']}: {e}")
                return None
            finally:
                await detail_page.close()

    def _extract_product_detail(self, html, body_text, product):
        soup = BeautifulSoup(html, "html.parser")
        normalized_body = self.normalize_text(body_text)

        raw_name = self._extract_title(soup, body_text)
        structured_fields = self.extract_structured_fields(raw_name)

        source_sku = self._extract_source_sku(product["source_url"])
        price = self._extract_price(normalized_body)
        brand = self._extract_brand(normalized_body)
        manufacturer = self._extract_labeled_value(normalized_body, "industria") or brand
        active_ingredient = self._extract_labeled_value(normalized_body, "principios ativos")
        dosage = structured_fields.get("dosage")
        pack_size = structured_fields.get("pack_size")
        ean_gtin = self._extract_ean(normalized_body)
        anvisa_code = self._extract_anvisa_code(normalized_body)
        promotion_text = None
        availability = "available"

        metadata = dict(product.get("source_metadata") or {})
        metadata["seller"] = self._extract_labeled_value(normalized_body, "vendido por")

        return {
            **product,
            "raw_name": raw_name,
            "normalized_name": structured_fields["normalized_name"],
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

    def _extract_title(self, soup, body_text):
        meta_title = soup.find("title")
        if meta_title and meta_title.get_text(strip=True):
            title = meta_title.get_text(" ", strip=True)
            title = re.sub(r"\s*-\s*farmacias?\s*app.*$", "", title, flags=re.I)
            if title.strip():
                return title.strip()

        match = re.search(r"Novalgina[^\nR$]{0,120}", body_text, re.I)
        if match:
            return match.group(0).strip()
        raise ValueError("Nome do produto nao encontrado na Farmacia Jaragua.")

    def _extract_source_sku(self, url):
        parts = [part for part in url.rstrip("/").split("/") if part]
        if len(parts) >= 2:
            return f"{parts[-2]}::{parts[-1]}"
        return parts[-1]

    def _extract_price(self, normalized_body):
        match = re.search(r"r\$\s*(\d+[.,]\d{2})\s*no pix", normalized_body)
        if not match:
            match = re.search(r"r\$\s*(\d+[.,]\d{2})", normalized_body)
        if match:
            return float(match.group(1).replace(",", "."))
        raise ValueError("Preco nao encontrado na Farmacia Jaragua.")

    def _extract_ean(self, normalized_body):
        match = re.search(r"ean\s*(\d{8,14})", normalized_body)
        return match.group(1) if match else None

    def _extract_anvisa_code(self, normalized_body):
        match = re.search(r"registro anvisa\s*(\d{8,13})", normalized_body)
        return match.group(1) if match else None

    def _extract_brand(self, normalized_body):
        brand = self._extract_labeled_value(normalized_body, "industria")
        if brand:
            return brand
        return self._extract_labeled_value(normalized_body, "vendido por")

    def _extract_labeled_value(self, normalized_body, label):
        match = re.search(
            rf"{label}\s*([a-z0-9\s\-/,'.]+?)(?=\s+(?:classe terapeutica|tipo de prescricao|tarja|tipo de medicamento|registro anvisa|principios ativos|ean|descricao|indicacao|contraindicacao|orientacoes gerais|precaucoes|vendido por|entregue por|$))",
            normalized_body,
        )
        if match:
            return match.group(1).strip()
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
            print("Nenhum produto da Farmacia Jaragua encontrado para salvar.")
            return

        session = SessionLocal()
        try:
            pharmacy = session.query(Pharmacy).filter_by(slug="farmacia-jaragua").first()
            if not pharmacy:
                raise ValueError("Farmacia Jaragua nao cadastrada. Rode src.init_db primeiro.")
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
            print(f"Sucesso: {len(products)} snapshots de precos da Farmacia Jaragua salvos no banco.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar produtos da Farmacia Jaragua no banco: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    scraper = FarmaciaJaraguaScraper()
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scraper.scrape())
    if data:
        scraper.save_to_db(data)
