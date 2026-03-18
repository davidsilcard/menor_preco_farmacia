import unittest

from src.main import (
    ObservedItemRequest,
    _build_observed_query,
    _build_price_summary,
    _estimate_overall_confidence,
    _has_special_token_conflict,
    _normalize_cep,
    _normalize_query,
    _tokenize_search_text,
    _tool_response,
)
from src.models.base import CanonicalProduct
from src.scrapers.base import BaseScraper
from src.services.matching import ProductMatcher


class _FakeQuery:
    def __init__(self, items):
        self.items = list(items)

    def filter_by(self, **kwargs):
        filtered = [
            item
            for item in self.items
            if all(getattr(item, key) == value for key, value in kwargs.items())
        ]
        return _FakeQuery(filtered)

    def first(self):
        return self.items[0] if self.items else None

    def all(self):
        return list(self.items)


class _FakeSession:
    def __init__(self, canonicals):
        self.canonicals = canonicals

    def query(self, model):
        if model is CanonicalProduct:
            return _FakeQuery(self.canonicals)
        raise AssertionError(f"Unexpected model query: {model}")


class ToolHelperTests(unittest.TestCase):
    def test_normalize_query_expands_common_aliases(self):
        normalized = _normalize_query("Dip Sod 1G 10 CPR Medley")
        self.assertIn("dipirona sodica", normalized)
        self.assertIn("comprimidos", normalized)

    def test_tokenize_search_text_removes_noise(self):
        tokens = _tokenize_search_text("cx novalg inf 100ml unid")
        self.assertIn("novalg", tokens)
        self.assertIn("infantil", tokens)
        self.assertNotIn("cx", tokens)
        self.assertNotIn("unid", tokens)

    def test_build_observed_query_ignores_lot_and_validity_tail(self):
        payload = ObservedItemRequest(
            cep="89254300",
            observations=[
                "Novalgina infantil dipirona 100ml",
                "Lote 12345",
                "Validade 12/2027",
            ],
            source_type="box_photo",
        )
        query = _build_observed_query(payload)
        self.assertIn("novalgina infantil dipirona 100ml", query)
        self.assertNotIn("lote", query)
        self.assertNotIn("validade", query)

    def test_normalize_cep_keeps_only_digits(self):
        self.assertEqual(_normalize_cep("89.254-300"), "89254300")

    def test_build_price_summary_returns_totals(self):
        items = [
            {
                "match_found": True,
                "requested_item": "novalgina 1g",
                "paid_price": 20.0,
                "quantity": 2,
                "potential_savings": 5.0,
                "best_offer": {"price": 17.5},
                "offers": [
                    {"pharmacy": "Panvel", "price": 17.5},
                    {"pharmacy": "Drogasil", "price": 18.0},
                ],
            }
        ]
        summary = _build_price_summary(items)
        self.assertEqual(summary["total_paid_informed"], 40.0)
        self.assertEqual(summary["total_best_available"], 35.0)
        self.assertEqual(summary["total_potential_savings"], 5.0)
        self.assertEqual(summary["best_basket_pharmacy"]["pharmacy"], "Panvel")
        self.assertEqual(summary["matched_items"], 1)
        self.assertEqual(summary["unmatched_items"], 0)

    def test_build_price_summary_excludes_incomplete_single_pharmacy_totals(self):
        items = [
            {
                "match_found": True,
                "requested_item": "novalgina 1g",
                "quantity": 1,
                "best_offer": {"price": 10.0},
                "offers": [
                    {"pharmacy": "Panvel", "price": 10.0},
                    {"pharmacy": "Drogasil", "price": 11.0},
                ],
            },
            {
                "match_found": True,
                "requested_item": "dipirona gotas",
                "quantity": 1,
                "best_offer": {"price": 8.0},
                "offers": [
                    {"pharmacy": "Panvel", "price": 8.0},
                ],
            },
        ]
        summary = _build_price_summary(items)
        self.assertEqual(summary["estimated_totals_by_pharmacy"], {"Panvel": 18.0})
        self.assertIn("Drogasil", summary["unavailable_items_by_pharmacy"])

    def test_estimate_overall_confidence_averages_matched_items(self):
        confidence = _estimate_overall_confidence(
            [
                {"match_found": True, "score": 80},
                {"match_found": True, "score": 60},
                {"match_found": False, "score": 0},
            ]
        )
        self.assertEqual(confidence, 0.7)

    def test_tool_response_has_standard_envelope(self):
        payload = _tool_response("search_products", {"query": "dipirona"}, {"results": []}, 0.2, ["warn"])
        self.assertEqual(payload["tool_name"], "search_products")
        self.assertEqual(payload["input"]["query"], "dipirona")
        self.assertEqual(payload["confidence"], 0.2)
        self.assertEqual(payload["warnings"], ["warn"])
        self.assertIn("result", payload)

    def test_special_token_conflict_blocks_wrong_form(self):
        self.assertTrue(
            _has_special_token_conflict(
                "novalgina 1g 10 comprimidos",
                "novalgina dipirona 1g 10 comprimidos efervescentes",
            )
        )
        self.assertFalse(
            _has_special_token_conflict(
                "novalgina gotas 20ml",
                "analgesico e antitermico novalgina 500mg/ml dipirona 20ml gotas",
            )
        )

    def test_clean_identifier_discards_known_placeholder_gtins(self):
        self.assertEqual(BaseScraper.clean_identifier("7891058002565"), "7891058002565")
        self.assertIsNone(BaseScraper.clean_identifier("9991234567890"))
        self.assertIsNone(BaseScraper.clean_identifier("0001234567890"))

    def test_matcher_auto_approves_structured_match_when_canonical_is_anchored(self):
        canonical = CanonicalProduct(
            canonical_name="Novalgina 1g 10 Comprimidos",
            normalized_name="novalgina 1g 10 comprimidos",
            brand="Novalgina",
            dosage="1g",
            presentation="comprimido",
            pack_size="10 comprimidos",
            ean_gtin="7891058001155",
        )
        matcher = ProductMatcher(_FakeSession([canonical]))

        decision = matcher.match_source_product(
            {
                "normalized_name": "novalgina 1g 10 comprimidos",
                "brand": "Novalgina",
                "dosage": "1g",
                "presentation": "comprimido",
                "pack_size": "10 comprimidos",
                "ean_gtin": None,
                "anvisa_code": None,
            }
        )

        self.assertEqual(decision.match_type, "anchored_normalized_name")
        self.assertEqual(decision.review_status, "auto_approved")
        self.assertEqual(decision.canonical_product, canonical)

    def test_matcher_normalizes_presentation_variants(self):
        matcher = ProductMatcher(_FakeSession([]))
        self.assertTrue(matcher._presentation_compatible("comprimidos", "comprimido"))
        self.assertTrue(matcher._presentation_compatible("gota", "gotas"))


if __name__ == "__main__":
    unittest.main()
