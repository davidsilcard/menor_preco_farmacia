import asyncio
import json
import random
import re
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from src.core.config import settings
from src.scrapers.base import BaseScraper


class PanvelScraper(BaseScraper):
    pharmacy_slug = "panvel"
    runtime_type = "browser"
    search_probe_format = "{base_domain}/panvel/buscarProduto.do?termoPesquisa={encoded_term}"
    search_probe_response_type = "html"
    search_probe_expected_content_type = "text/html"
    search_probe_contains_term = True

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
                self.log_info("scraper_search_requested", term=term, search_url=search_url)
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

                self.log_info("scraper_cep_configured")
                await asyncio.sleep(random.uniform(3, 5))
        except Exception as e:
            self.log_warning("scraper_cep_configuration_skipped", error_message=str(e)[:500])

    async def _collect_listing_products(self, page, term):
        products_found = []

        await page.wait_for_selector('a[href*="/p-"]', timeout=15000)
        product_links = await page.query_selector_all('a[href*="/p-"]')

        seen_skus = set()
        self.log_info("scraper_listing_candidates_found", listing_candidates=len(product_links), term=term)

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
                        "availability": "unknown",
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
        availability = self.availability_from_schema(product_schema, normalized_body)
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
            "availability": availability,
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
        self.save_products_to_db(products)

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
