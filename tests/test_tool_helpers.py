import unittest

from src.main import (
    ObservedItemRequest,
    _availability_rank,
    _availability_warnings,
    _best_pricing_offer,
    _build_observed_query,
    _build_price_summary,
    _estimate_overall_confidence,
    _has_special_token_conflict,
    _normalize_cep,
    _normalize_query,
    _score_canonical_match,
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
        self.added = []

    def query(self, model):
        if model is CanonicalProduct:
            return _FakeQuery(self.canonicals)
        raise AssertionError(f"Unexpected model query: {model}")

    def add(self, instance):
        self.added.append(instance)

    def flush(self):
        return None


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

    def test_best_offer_ignores_out_of_stock(self):
        best_offer = _best_pricing_offer(
            [
                {"pharmacy": "Panvel", "price": 10.0, "availability": "out_of_stock"},
                {"pharmacy": "Drogasil", "price": 11.0, "availability": "available"},
            ]
        )
        self.assertEqual(best_offer["pharmacy"], "Drogasil")

    def test_build_price_summary_treats_out_of_stock_as_unavailable(self):
        items = [
            {
                "match_found": True,
                "requested_item": "novalgina 1g",
                "quantity": 1,
                "best_offer": {"price": 11.0, "pharmacy": "Drogasil", "availability": "available"},
                "offers": [
                    {"pharmacy": "Panvel", "price": 10.0, "availability": "out_of_stock"},
                    {"pharmacy": "Drogasil", "price": 11.0, "availability": "available"},
                ],
            }
        ]
        summary = _build_price_summary(items)
        self.assertEqual(summary["estimated_totals_by_pharmacy"], {"Drogasil": 11.0})
        self.assertEqual(summary["unavailable_items_by_pharmacy"]["Panvel"], ["novalgina 1g"])

    def test_best_offer_prefers_available_over_unknown(self):
        best_offer = _best_pricing_offer(
            [
                {"pharmacy": "Panvel", "price": 10.0, "availability": "unknown"},
                {"pharmacy": "Drogasil", "price": 11.0, "availability": "available"},
            ]
        )
        self.assertEqual(best_offer["pharmacy"], "Drogasil")

    def test_availability_rank_orders_available_before_unknown(self):
        self.assertLess(_availability_rank("available"), _availability_rank("unknown"))
        self.assertLess(_availability_rank("unknown"), _availability_rank("out_of_stock"))

    def test_availability_warnings_flag_unknown_and_out_of_stock(self):
        warnings = _availability_warnings(
            [
                {
                    "match_found": True,
                    "requested_item": "novalgina 1g",
                    "best_offer": None,
                    "offers": [{"pharmacy": "Panvel", "price": 10.0, "availability": "out_of_stock"}],
                },
                {
                    "match_found": True,
                    "requested_item": "dipirona gotas",
                    "best_offer": {"pharmacy": "Drogasil", "price": 11.0, "availability": "unknown"},
                    "offers": [{"pharmacy": "Drogasil", "price": 11.0, "availability": "unknown"}],
                },
            ]
        )
        self.assertTrue(any("sem estoque" in warning for warning in warnings))
        self.assertTrue(any("nao confirmado" in warning for warning in warnings))

    def test_build_price_summary_does_not_claim_full_basket_when_item_has_only_out_of_stock(self):
        items = [
            {
                "match_found": True,
                "requested_item": "novalgina 1g",
                "quantity": 1,
                "best_offer": {"price": 11.0, "pharmacy": "Drogasil", "availability": "available"},
                "offers": [{"pharmacy": "Drogasil", "price": 11.0, "availability": "available"}],
            },
            {
                "match_found": True,
                "requested_item": "mounjaro 15mg",
                "quantity": 1,
                "best_offer": None,
                "offers": [{"pharmacy": "Drogaria Catarinense", "price": 3590.0, "availability": "out_of_stock"}],
            },
        ]
        summary = _build_price_summary(items)
        self.assertIsNone(summary["best_basket_pharmacy"])
        self.assertIn("mounjaro 15mg", summary["unavailable_items_by_pharmacy"]["Drogasil"])

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
        self.assertFalse(
            _has_special_token_conflict(
                "7896382709210",
                "mounjaro tirzepatida 15mg/0,5ml com 4 canetas de 0,5ml solucao injetavel eli lilly",
            )
        )

    def test_clean_identifier_discards_known_placeholder_gtins(self):
        self.assertEqual(BaseScraper.clean_identifier("7891058002565"), "7891058002565")
        self.assertIsNone(BaseScraper.clean_identifier("9991234567890"))
        self.assertIsNone(BaseScraper.clean_identifier("0001234567890"))

    def test_extract_structured_fields_does_not_treat_volume_only_as_dosage(self):
        fields = BaseScraper.extract_structured_fields("Novalgina Gotas 20ml")
        self.assertIsNone(fields["dosage"])
        self.assertEqual(fields["pack_size"], "20ml")

    def test_availability_from_quantity_distinguishes_zero_stock(self):
        self.assertEqual(BaseScraper.availability_from_quantity(3), "available")
        self.assertEqual(BaseScraper.availability_from_quantity(0), "out_of_stock")
        self.assertEqual(BaseScraper.availability_from_quantity(None), "unknown")

    def test_availability_from_text_detects_indisponibilidade(self):
        self.assertEqual(BaseScraper.availability_from_text("Produto indisponivel no momento"), "out_of_stock")
        self.assertEqual(BaseScraper.availability_from_text("Adicionar ao carrinho"), "available")

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

    def test_matcher_prefers_anchored_candidate_over_legacy_unanchored_exact_name(self):
        legacy = CanonicalProduct(
            id=1,
            canonical_name="Novalgina Gotas 20ml",
            normalized_name="novalgina gotas 20ml",
            presentation="gotas",
            pack_size="20ml",
        )
        anchored = CanonicalProduct(
            id=2,
            canonical_name="Analgésico e Antitérmico Novalgina 500mg/ml Dipirona 20ml Gotas",
            normalized_name="analgesico e antitermico novalgina 500mg/ml dipirona 20ml gotas",
            dosage="500mg",
            presentation="gotas",
            pack_size="20ml",
            ean_gtin="7891058000165",
        )
        matcher = ProductMatcher(_FakeSession([legacy, anchored]))

        decision = matcher.match_source_product(
            {
                "normalized_name": "novalgina gotas 20ml",
                "brand": None,
                "dosage": "500mg",
                "presentation": "gotas",
                "pack_size": "20ml",
                "ean_gtin": None,
                "anvisa_code": None,
            }
        )

        self.assertEqual(decision.match_type, "anchored_structured_match")
        self.assertEqual(decision.review_status, "auto_approved")
        self.assertEqual(decision.canonical_product, anchored)

    def test_build_canonical_product_reuses_existing_anchored_record(self):
        existing = CanonicalProduct(
            id=33,
            canonical_name="Mounjaro Tirzepatida 15mg/0,5ml",
            normalized_name="mounjaro tirzepatida 15mg/0,5ml",
            ean_gtin="7896382709210",
        )
        session = _FakeSession([existing])
        matcher = ProductMatcher(session)

        canonical = matcher.build_canonical_product(
            {
                "raw_name": "Mounjaro Tirzepatida 15mg/0,5ml",
                "normalized_name": "mounjaro tirzepatida 15mg/0,5ml",
                "ean_gtin": "7896382709210",
                "anvisa_code": None,
            }
        )

        self.assertEqual(canonical, existing)
        self.assertEqual(session.added, [])

    def test_score_canonical_match_supports_ean_and_partial_dosage(self):
        canonical = CanonicalProduct(
            canonical_name="Mounjaro Tirzepatida 15mg/0,5ml Com 4 Canetas De 0,5ml Solucao Injetavel Eli Lilly",
            normalized_name="mounjaro tirzepatida 15mg/0,5ml com 4 canetas de 0,5ml solucao injetavel eli lilly",
            dosage="15mg/0,5ml",
            ean_gtin="7896382709210",
        )
        self.assertGreater(_score_canonical_match(canonical, "7896382709210"), 0)
        self.assertGreater(_score_canonical_match(canonical, "mounjaro 15mg"), 0)


if __name__ == "__main__":
    unittest.main()
