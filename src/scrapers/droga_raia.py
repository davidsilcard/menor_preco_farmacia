import asyncio
import json
import random
import re
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.core.config import settings
from src.scrapers.base import BaseScraper


class DrogaRaiaScraper(BaseScraper):
    pharmacy_slug = "droga-raia"
    runtime_type = "browser"
    search_probe_format = "{base_domain}/search?w={encoded_term}"
    search_probe_response_type = "html"
    search_probe_expected_content_type = "text/html"
    search_probe_contains_term = True

    def __init__(self):
        super().__init__("https://www.drogaraia.com.br")
        self.base_domain = "https://www.drogaraia.com.br"
        self.search_terms = [term.strip() for term in settings.DROGA_RAIA_SEARCH_TERMS.split(",") if term.strip()]
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
                self.log_info("scraper_search_requested", term=term, search_url=search_url)
                await page.goto(search_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(random.uniform(2, 3))

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

        self.log_info("scraper_term_collected", term=term, products_found=len(products))
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
                self.log_info(
                    "scraper_detail_collected",
                    source_sku=enriched["source_sku"],
                    ean_gtin=enriched.get("ean_gtin"),
                    anvisa_code=enriched.get("anvisa_code"),
                )
                return enriched
            except Exception as e:
                self.log_warning(
                    "scraper_detail_enrichment_failed",
                    source_url=product["source_url"],
                    error_message=str(e)[:500],
                )
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
        raise ValueError("Preco nao encontrado na pagina da Droga Raia.")

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
        self.save_products_to_db(products, required_fields=("source_sku", "price"))

    @staticmethod
    def _first_non_empty(*values):
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None


if __name__ == "__main__":
    scraper = DrogaRaiaScraper()
    loop = asyncio.get_event_loop()
    data = loop.run_until_complete(scraper.scrape())
    if data:
        scraper.save_to_db(data)
