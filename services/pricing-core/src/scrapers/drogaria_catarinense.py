import json
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from src.core.config import settings
from src.scrapers.base import BaseScraper


class DrogariaCatarinenseScraper(BaseScraper):
    pharmacy_slug = "drogaria-catarinense"
    runtime_type = "http"
    search_probe_format = "{base_domain}/api/catalog_system/pub/products/search?ft={encoded_term}"
    search_probe_response_type = "json"
    search_probe_expected_content_type = "application/json"
    search_probe_expected_json_root = "list"

    def __init__(self):
        super().__init__("https://www.drogariacatarinense.com.br")
        self.base_domain = "https://www.drogariacatarinense.com.br"
        self.search_terms = [term.strip() for term in settings.CATARINENSE_SEARCH_TERMS.split(",") if term.strip()]
        self.max_products_per_term = 12

    def _fetch_products_for_term(self, term: str):
        url = f"{self.base_domain}/api/catalog_system/pub/products/search?ft={quote_plus(term)}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))

        products = []
        for product in payload[: self.max_products_per_term]:
            items = product.get("items") or []
            if not items:
                continue

            sku_item = items[0]
            sellers = sku_item.get("sellers") or []
            if not sellers:
                continue

            offer = sellers[0].get("commertialOffer") or {}
            price = offer.get("Price")
            if price in (None, 0):
                continue

            raw_name = product.get("productName") or product.get("productTitle")
            if not raw_name:
                continue

            structured_fields = self.extract_structured_fields(raw_name)
            reference_ids = sku_item.get("referenceId") or []
            anvisa_code = None
            for reference in reference_ids:
                key = (reference.get("Key") or "").lower()
                value = reference.get("Value")
                if key in {"registro ms", "anvisa", "ms"} and value:
                    anvisa_code = value
                    break

            products.append(
                {
                    "raw_name": raw_name,
                    "source_sku": str(sku_item.get("itemId") or product.get("productReference") or product.get("productId")),
                    "price": float(price),
                    "source_url": product.get("link"),
                    "availability": self.availability_from_quantity(offer.get("AvailableQuantity")),
                    "brand": product.get("brand"),
                    "manufacturer": product.get("brand"),
                    "active_ingredient": None,
                    "ean_gtin": sku_item.get("ean") or None,
                    "anvisa_code": anvisa_code,
                    "promotion_text": None,
                    "source_metadata": {
                        "search_term": term,
                        "product_id": product.get("productId"),
                        "product_reference": product.get("productReference"),
                        "categories": product.get("categories"),
                        "available_quantity": offer.get("AvailableQuantity"),
                    },
                    **structured_fields,
                }
            )

        self.log_info("scraper_term_collected", term=term, products_found=len(products))
        return products

    def scrape(self):
        all_products = []
        seen_skus = set()

        for term in self.search_terms:
            products = self._fetch_products_for_term(term)
            for product in products:
                sku = product["source_sku"]
                if sku in seen_skus:
                    continue
                seen_skus.add(sku)
                all_products.append(product)

        return all_products

    def save_to_db(self, products):
        self.save_products_to_db(products)


if __name__ == "__main__":
    scraper = DrogariaCatarinenseScraper()
    data = scraper.scrape()
    if data:
        scraper.save_to_db(data)
