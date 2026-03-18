import json
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from src.core.config import settings
from src.models.base import Pharmacy, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class DrogariaSaoPauloScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://www.drogariasaopaulo.com.br")
        self.base_domain = "https://www.drogariasaopaulo.com.br"
        self.search_terms = [
            term.strip() for term in settings.DROGARIA_SAO_PAULO_SEARCH_TERMS.split(",") if term.strip()
        ]
        self.max_products_per_term = 12

    def _fetch_products_for_term(self, term: str):
        url = f"{self.base_domain}/api/catalog_system/pub/products/search?ft={quote_plus(term)}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
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
                    "availability": "available" if offer.get("AvailableQuantity", 0) > 0 else "unknown",
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

        print(f"Drogaria Sao Paulo: {len(products)} produtos coletados para o termo '{term}'.")
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
        if not products:
            print("Nenhum produto da Drogaria Sao Paulo encontrado para salvar.")
            return

        session = SessionLocal()
        try:
            pharmacy = session.query(Pharmacy).filter_by(slug="drogaria-sao-paulo").first()
            if not pharmacy:
                raise ValueError("Farmacia Drogaria Sao Paulo nao cadastrada. Rode src.init_db primeiro.")
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
            print(f"Sucesso: {len(products)} snapshots de precos da Drogaria Sao Paulo salvos no banco.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar produtos da Drogaria Sao Paulo no banco: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    scraper = DrogariaSaoPauloScraper()
    data = scraper.scrape()
    if data:
        scraper.save_to_db(data)
