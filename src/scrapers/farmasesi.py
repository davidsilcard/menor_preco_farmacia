import json
import re
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from src.core.config import settings
from src.models.base import Pharmacy, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class FarmaSesiScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://www.farmasesi.com.br/busca?busca=dipirona")
        self.base_domain = "https://www.farmasesi.com.br"
        self.search_terms = [term.strip() for term in settings.FARMASESI_SEARCH_TERMS.split(",") if term.strip()]
        self.max_products_per_term = 16

    def _fetch_html(self, url: str):
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=30) as response:
            return response.read().decode("utf-8", "ignore")

    def _extract_listing_products(self, term: str):
        url = f"{self.base_domain}/busca?busca={quote_plus(term)}"
        html = self._fetch_html(url)
        soup = BeautifulSoup(html, "html.parser")
        term_tokens = [token for token in self.normalize_text(term).split() if len(token) >= 4]

        products = []
        seen_codes = set()
        for item in soup.select(".product-item[data-cod]"):
            source_sku = item.get("data-cod")
            link = item.select_one('a[href*="/produto/"]')
            title = item.select_one("h2")
            if not source_sku or not link or not title:
                continue

            raw_name = title.get_text(" ", strip=True)
            normalized_name = self.normalize_text(raw_name)
            if term_tokens and not any(token in normalized_name for token in term_tokens):
                continue
            if source_sku in seen_codes:
                continue
            seen_codes.add(source_sku)

            full_url = link.get("href")
            if full_url and not full_url.startswith("http"):
                full_url = f"{self.base_domain}{full_url}"

            products.append(
                {
                    "source_sku": str(source_sku),
                    "raw_name": raw_name,
                    "source_url": full_url,
                    "source_metadata": {
                        "search_term": term,
                        "search_url": url,
                    },
                    **self.extract_structured_fields(raw_name),
                }
            )

        return products[: self.max_products_per_term]

    def _fetch_listing_prices(self, product_codes):
        if not product_codes:
            return {}

        payload = f"modulo=dadosProdutos&codigos={','.join(product_codes)},"
        req = Request(
            f"{self.base_domain}/ajax/header/?m=dadosProdutos",
            data=payload.encode("utf-8"),
            headers={
                "User-Agent": "Mozilla/5.0",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        with urlopen(req, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))

        return {str(item.get("produto")): item for item in data if item.get("produto")}

    def _extract_detail_fields(self, html: str):
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text(" ", strip=True)
        normalized_body = self.normalize_text(body_text)
        product_schema = self._extract_product_schema(soup)

        brand = self._extract_brand(product_schema, html, normalized_body)
        manufacturer = self._extract_labeled_value(normalized_body, "fabricante|laboratorio|industria")
        active_ingredient = self._extract_labeled_value(normalized_body, "principio ativo|substancia ativa")
        dosage = self._extract_labeled_value(normalized_body, "dosagem")
        pack_size = self._extract_labeled_value(normalized_body, "quantidade")

        price_match = re.search(r"productPrice['\"]?\s*:\s*['\"](\d+[.,]\d+)['\"]", html, re.I)
        page_price = float(price_match.group(1).replace(",", ".")) if price_match else None

        ean_gtin = self._extract_ean(product_schema, normalized_body)
        anvisa_code = self._extract_anvisa_code(normalized_body)

        return {
            "brand": brand,
            "manufacturer": manufacturer or brand,
            "active_ingredient": active_ingredient,
            "dosage": dosage,
            "pack_size": pack_size,
            "ean_gtin": ean_gtin,
            "anvisa_code": anvisa_code,
            "page_price": page_price,
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

    def _extract_brand(self, product_schema, html: str, normalized_body: str):
        if product_schema:
            brand = product_schema.get("brand")
            if isinstance(brand, dict):
                return brand.get("name")
            if isinstance(brand, str):
                return brand

        brand_match = re.search(r"productBrand['\"]?\s*:\s*['\"]([^'\"]+)['\"]", html, re.I)
        if brand_match:
            return brand_match.group(1).strip()

        return self._extract_labeled_value(normalized_body, "marca")

    def _extract_ean(self, product_schema, normalized_body: str):
        if product_schema:
            for key in ("gtin13", "gtin", "gtin14"):
                digits = self.clean_identifier(product_schema.get(key))
                if digits:
                    return digits
        match = re.search(r"\b(?:ean\.?|ean:|codigo de barras|gtin)\D{0,16}(\d{8,14})\b", normalized_body, re.I)
        return self.clean_identifier(match.group(1)) if match else None

    def _extract_anvisa_code(self, normalized_body: str):
        match = re.search(r"\b(?:registro ms|registro anvisa|anvisa|ms)\D{0,20}(\d{8,13})\b", normalized_body, re.I)
        return match.group(1) if match else None

    def _extract_labeled_value(self, normalized_body: str, label_pattern: str):
        match = re.search(
            rf"(?:{label_pattern})\s+([a-z0-9\s\-/,'.]+?)(?=\s+(?:marca|quantidade|principio ativo|substancia ativa|caracteristicas|codigo do produto|fabricante|laboratorio|industria|registro ms|registro anvisa|dosagem|ean|codigo de barras|gtin|$))",
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

    def scrape(self):
        all_products = []
        seen_skus = set()

        for term in self.search_terms:
            listing_products = self._extract_listing_products(term)
            price_map = self._fetch_listing_prices([product["source_sku"] for product in listing_products])

            for product in listing_products:
                listing_price = price_map.get(product["source_sku"])
                if not listing_price:
                    continue

                detail_html = self._fetch_html(product["source_url"])
                detail_fields = self._extract_detail_fields(detail_html)

                final_price = listing_price.get("valorFinal") or detail_fields.get("page_price")
                if final_price in (None, "", 0):
                    continue

                structured_fields = self.extract_structured_fields(product["raw_name"])
                product_data = {
                    **product,
                    **structured_fields,
                    "price": float(final_price),
                    "availability": "available" if int(listing_price.get("qtd") or 0) > 0 else "unknown",
                    "brand": detail_fields.get("brand"),
                    "manufacturer": detail_fields.get("manufacturer"),
                    "active_ingredient": detail_fields.get("active_ingredient"),
                    "dosage": detail_fields.get("dosage") or structured_fields.get("dosage"),
                    "pack_size": detail_fields.get("pack_size") or structured_fields.get("pack_size"),
                    "ean_gtin": detail_fields.get("ean_gtin"),
                    "anvisa_code": detail_fields.get("anvisa_code"),
                    "promotion_text": None,
                    "source_metadata": {
                        **(product.get("source_metadata") or {}),
                        "filial": listing_price.get("filial"),
                        "available_quantity": listing_price.get("qtd"),
                        "list_price": listing_price.get("valor"),
                        "final_price": listing_price.get("valorFinal"),
                    },
                }

                if not self._matches_search_term(product_data):
                    continue
                if product_data["source_sku"] in seen_skus:
                    continue
                seen_skus.add(product_data["source_sku"])
                all_products.append(product_data)

            print(f"FarmaSesi: {len([p for p in all_products if p['source_metadata'].get('search_term') == term])} produtos coletados para o termo '{term}'.")

        return all_products

    def save_to_db(self, products):
        if not products:
            print("Nenhum produto da FarmaSesi encontrado para salvar.")
            return

        session = SessionLocal()
        try:
            pharmacy = session.query(Pharmacy).filter_by(slug="farmasesi").first()
            if not pharmacy:
                raise ValueError("Farmacia FarmaSesi nao cadastrada. Rode src.init_db primeiro.")
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
            print(f"Sucesso: {len(products)} snapshots de precos da FarmaSesi salvos no banco.")
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar produtos da FarmaSesi no banco: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    scraper = FarmaSesiScraper()
    data = scraper.scrape()
    if data:
        scraper.save_to_db(data)
