import logging
import re
import unicodedata
from datetime import UTC, datetime
from urllib.parse import quote_plus

from src.core.config import settings
from src.core.logging import get_logger, log_event
from src.models.base import Pharmacy, PriceSnapshot, ProductMatch, ScrapeRun, SessionLocal, SourceProduct
from src.services.matching import ProductMatcher


class BaseScraper:
    SOURCE_PRODUCT_FIELDS = (
        "source_url",
        "raw_name",
        "normalized_name",
        "brand",
        "manufacturer",
        "active_ingredient",
        "dosage",
        "presentation",
        "pack_size",
        "ean_gtin",
        "anvisa_code",
        "source_metadata",
    )

    pharmacy_slug = None
    runtime_type = "http"
    search_probe_format = None
    search_probe_response_type = "html"
    search_probe_expected_content_type = "text/html"
    search_probe_expected_json_root = None
    search_probe_contains_term = False

    OUT_OF_STOCK_PATTERNS = (
        "indisponivel",
        "indisponivel no momento",
        "fora de estoque",
        "sem estoque",
        "produto esgotado",
        "esgotado",
        "avise me",
    )

    IN_STOCK_PATTERNS = (
        "instock",
        "in stock",
        "retirar hoje",
        "comprar",
        "adicionar ao carrinho",
    )

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.cep = self.normalize_cep(settings.CEP)
        self.logger = get_logger(self.__class__.__module__)

    @staticmethod
    def normalize_cep(value: str | None) -> str:
        return re.sub(r"\D", "", value or "")

    def set_cep(self, cep: str | None):
        normalized_cep = self.normalize_cep(cep)
        if not normalized_cep:
            raise ValueError("CEP e obrigatorio para executar scraper.")
        self.cep = normalized_cep
        return self

    @property
    def pharmacy_name(self) -> str:
        slug = self.pharmacy_slug or self.__class__.__name__.replace("Scraper", "")
        return " ".join(chunk.capitalize() for chunk in slug.split("-"))

    def runtime_enabled(self) -> bool:
        if self.runtime_type == "http":
            return True
        return settings.ON_DEMAND_ENABLE_BROWSER_SCRAPERS or settings.SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS

    def build_probe_specs(self, sample_term: str = "dipirona") -> list[dict]:
        base_domain = getattr(self, "base_domain", self.base_url)
        specs = [
            {
                "probe_name": "homepage",
                "url": base_domain,
                "response_type": "html",
                "expected_content_type": "text/html",
            }
        ]

        if not self.search_probe_format:
            return specs

        search_spec = {
            "probe_name": "search_probe",
            "url": self.search_probe_format.format(base_domain=base_domain, encoded_term=quote_plus(sample_term)),
            "response_type": self.search_probe_response_type,
            "expected_content_type": self.search_probe_expected_content_type,
        }
        if self.search_probe_expected_json_root:
            search_spec["expected_json_root"] = self.search_probe_expected_json_root
        if self.search_probe_contains_term:
            search_spec["contains_any"] = [sample_term.lower()]
        specs.append(search_spec)
        return specs

    def _log(self, level: int, event: str, **fields):
        log_event(
            self.logger,
            level,
            event,
            pharmacy_slug=self.pharmacy_slug,
            pharmacy=self.pharmacy_name,
            cep=self.cep,
            **fields,
        )

    def log_info(self, event: str, **fields):
        self._log(logging.INFO, event, **fields)

    def log_warning(self, event: str, **fields):
        self._log(logging.WARNING, event, **fields)

    def log_error(self, event: str, **fields):
        self._log(logging.ERROR, event, **fields)

    async def get_browser_context(self, playwright):
        # Usando Firefox temporariamente por problemas no download do Chromium da Playwright
        browser = await playwright.firefox.launch(headless=True) 
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0"
        )
        return browser, context

    @staticmethod
    def normalize_text(value: str) -> str:
        normalized = unicodedata.normalize("NFKD", value or "")
        normalized = normalized.encode("ascii", "ignore").decode("ascii")
        normalized = re.sub(r"\s+", " ", normalized).strip().lower()
        return normalized

    @staticmethod
    def clean_identifier(value: str | None, valid_lengths: tuple[int, ...] = (8, 12, 13, 14)) -> str | None:
        if value is None:
            return None
        digits = re.sub(r"\D", "", str(value))
        if len(digits) not in valid_lengths:
            return None
        if digits.startswith(("000", "999")):
            return None
        return digits or None

    @classmethod
    def availability_from_quantity(cls, quantity) -> str:
        if quantity is None or quantity == "":
            return "unknown"
        try:
            return "available" if float(quantity) > 0 else "out_of_stock"
        except (TypeError, ValueError):
            return "unknown"

    @classmethod
    def availability_from_text(cls, value: str | None) -> str:
        normalized = cls.normalize_text(value or "")
        if not normalized:
            return "unknown"
        if any(pattern in normalized for pattern in cls.OUT_OF_STOCK_PATTERNS):
            return "out_of_stock"
        if any(pattern in normalized for pattern in cls.IN_STOCK_PATTERNS):
            return "available"
        return "unknown"

    @classmethod
    def availability_from_schema(cls, product_schema, fallback_text: str | None = None) -> str:
        if product_schema:
            offers = product_schema.get("offers") or {}
            availability = None
            if isinstance(offers, dict):
                availability = offers.get("availability")
                nested_offers = offers.get("offers")
                if not availability and isinstance(nested_offers, list) and nested_offers:
                    availability = nested_offers[0].get("availability")
            elif isinstance(offers, list) and offers:
                availability = offers[0].get("availability")

            normalized_availability = cls.normalize_text(str(availability or ""))
            if normalized_availability:
                if "outofstock" in normalized_availability:
                    return "out_of_stock"
                if "instock" in normalized_availability:
                    return "available"

        return cls.availability_from_text(fallback_text)

    @staticmethod
    def extract_structured_fields(raw_name: str) -> dict:
        normalized = BaseScraper.normalize_text(raw_name)

        dosage_match = re.search(r"(\d+[.,]?\d*\s?(?:mg|mcg|g|ui)(?:/\s?\d+[.,]?\d*\s?ml)?)", normalized, re.IGNORECASE)
        pack_match = re.search(r"(\d+\s?(comprimidos?|capsulas?|caps|ml|unidades?|saches?|ampolas?))", normalized, re.IGNORECASE)

        presentation = None
        for candidate in ("comprimido", "capsula", "xarope", "gotas", "pomada", "creme", "solucao", "spray"):
            if candidate in normalized:
                presentation = candidate
                break

        return {
            "normalized_name": normalized,
            "dosage": dosage_match.group(1) if dosage_match else None,
            "pack_size": pack_match.group(1) if pack_match else None,
            "presentation": presentation,
        }

    def start_scrape_run(self, session, pharmacy, search_terms, trigger_type: str = "scheduled"):
        scrape_run = ScrapeRun(
            pharmacy_id=pharmacy.id,
            cep=self.cep,
            trigger_type=trigger_type,
            status="running",
            search_terms=list(search_terms or []),
        )
        session.add(scrape_run)
        session.commit()
        session.refresh(scrape_run)
        return scrape_run

    @staticmethod
    def update_scrape_run(
        session,
        scrape_run_id: int,
        *,
        status: str,
        products_seen: int,
        products_saved: int,
        error_count: int = 0,
        error_message: str | None = None,
    ):
        scrape_run = session.get(ScrapeRun, scrape_run_id)
        if not scrape_run:
            return
        scrape_run.status = status
        scrape_run.products_seen = products_seen
        scrape_run.products_saved = products_saved
        scrape_run.error_count = error_count
        scrape_run.error_message = error_message
        scrape_run.finished_at = datetime.now(UTC).replace(tzinfo=None)

    @staticmethod
    def _missing_required_product_fields(product_data: dict, required_fields: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(field for field in required_fields if not product_data.get(field))

    def _upsert_source_product(self, session, pharmacy_id: int, product_data: dict) -> SourceProduct:
        source_product = (
            session.query(SourceProduct)
            .filter_by(pharmacy_id=pharmacy_id, source_sku=product_data["source_sku"])
            .first()
        )

        if not source_product:
            source_product = SourceProduct(
                pharmacy_id=pharmacy_id,
                source_sku=product_data["source_sku"],
            )
            session.add(source_product)
            session.flush()

        for field_name in self.SOURCE_PRODUCT_FIELDS:
            setattr(source_product, field_name, product_data.get(field_name))

        return source_product

    @staticmethod
    def _resolve_match(matcher: ProductMatcher, product_data: dict):
        decision = matcher.match_source_product(product_data)
        canonical_product = decision.canonical_product or matcher.build_canonical_product(product_data)
        decision = matcher.resolve_match_metadata(canonical_product, product_data)
        return canonical_product, decision

    @staticmethod
    def _upsert_product_match(session, source_product: SourceProduct, canonical_product, decision):
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
            return

        source_product.match.canonical_product_id = canonical_product.id
        source_product.match.match_type = decision.match_type
        source_product.match.confidence = decision.confidence
        source_product.match.review_status = decision.review_status
        source_product.match.review_notes = decision.review_notes

    def _create_price_snapshot(self, source_product: SourceProduct, scrape_run_id: int, product_data: dict) -> PriceSnapshot:
        return PriceSnapshot(
            source_product_id=source_product.id,
            scrape_run_id=scrape_run_id,
            price=product_data["price"],
            cep=self.cep,
            availability=product_data.get("availability", "unknown"),
            source_url=product_data.get("source_url"),
            promotion_text=product_data.get("promotion_text"),
        )

    def save_products_to_db(self, products, *, required_fields: tuple[str, ...] = ()):
        if not products:
            self.log_info("scraper_save_skipped_no_products")
            return

        session = SessionLocal()
        scrape_run = None
        products_seen = len(products)
        products_saved = 0
        skipped_products = 0

        try:
            pharmacy = session.query(Pharmacy).filter_by(slug=self.pharmacy_slug).first()
            if not pharmacy:
                raise ValueError(f"Farmacia {self.pharmacy_name} nao cadastrada. Rode src.init_db primeiro.")

            matcher = ProductMatcher(session)
            scrape_run = self.start_scrape_run(session, pharmacy, getattr(self, "search_terms", []))

            for product_data in products:
                missing_fields = self._missing_required_product_fields(product_data, required_fields)
                if missing_fields:
                    skipped_products += 1
                    continue

                source_product = self._upsert_source_product(session, pharmacy.id, product_data)
                canonical_product, decision = self._resolve_match(matcher, product_data)
                self._upsert_product_match(session, source_product, canonical_product, decision)
                matcher.reconcile_canonical_matches(canonical_product)
                session.add(self._create_price_snapshot(source_product, scrape_run.id, product_data))
                products_saved += 1

            self.update_scrape_run(
                session,
                scrape_run.id,
                status="completed",
                products_seen=products_seen,
                products_saved=products_saved,
            )
            session.commit()
            self.log_info(
                "scraper_save_completed",
                products_seen=products_seen,
                products_saved=products_saved,
                skipped_products=skipped_products,
            )
        except Exception as exc:
            session.rollback()
            if scrape_run:
                self.update_scrape_run(
                    session,
                    scrape_run.id,
                    status="failed",
                    products_seen=products_seen,
                    products_saved=0,
                    error_count=1,
                    error_message=str(exc)[:500],
                )
                session.commit()
            self.log_error("scraper_save_failed", error_message=str(exc)[:500])
        finally:
            session.close()
