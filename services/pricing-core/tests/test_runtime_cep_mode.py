import unittest
from unittest.mock import patch

from src.core.config import settings
from src.scrapers.base import BaseScraper
from src.services.ops import _config_readiness_payload


class _DummyScraper(BaseScraper):
    def __init__(self):
        super().__init__("https://example.com")


class RuntimeCepModeTests(unittest.TestCase):
    def test_readiness_allows_runtime_without_default_cep(self):
        with patch.object(settings, "CEP", ""):
            payload = _config_readiness_payload()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["configured_default_cep"], None)
        self.assertEqual(payload["runtime_cep_mode"], "request_scoped")
        self.assertFalse(payload["default_cep_configured"])

    def test_readiness_rejects_invalid_optional_default_cep(self):
        with patch.object(settings, "CEP", "123"):
            payload = _config_readiness_payload()

        self.assertEqual(payload["status"], "error")
        self.assertIn("CEP padrao opcional invalido", payload["issues"][0])

    def test_base_scraper_does_not_bind_global_cep_on_init(self):
        with patch.object(settings, "CEP", "89254300"):
            scraper = _DummyScraper()

        self.assertIsNone(scraper.cep)


if __name__ == "__main__":
    unittest.main()
