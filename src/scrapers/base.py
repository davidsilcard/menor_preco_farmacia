import re
import unicodedata

from src.core.config import settings


class BaseScraper:
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

    @staticmethod
    def extract_structured_fields(raw_name: str) -> dict:
        normalized = BaseScraper.normalize_text(raw_name)

        dosage_match = re.search(r"(\d+[.,]?\d*\s?(mg|mcg|g|ml|ui))", normalized, re.IGNORECASE)
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
