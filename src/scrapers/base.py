import re
import unicodedata
from datetime import datetime

from src.core.config import settings
from src.models.base import ScrapeRun


class BaseScraper:
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
        self.cep = settings.CEP

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
        scrape_run.finished_at = datetime.utcnow()
