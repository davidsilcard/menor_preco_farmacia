import unittest
from datetime import UTC, datetime, timedelta

from src.main import (
    ObservedItemRequest,
    _availability_rank,
    _basket_availability_summary,
    _basket_freshness_summary,
    _availability_warnings,
    _best_pricing_offer,
    _build_basket_result,
    _build_observed_query,
    _build_price_summary,
    _freshness_status,
    _estimate_overall_confidence,
    _has_special_token_conflict,
    _item_availability_summary,
    _normalize_cep,
    _normalize_query,
    _ops_health_payload,
    _pharmacy_metrics,
    _queue_metrics,
    _register_catalog_request,
    _register_search_job,
    _search_job_payload,
    _score_canonical_match,
    _snapshot_freshness_payload,
    _tokenize_search_text,
    _tool_response,
)
from src.models.base import CanonicalProduct
from src.models.base import CatalogRequest, Pharmacy, PriceSnapshot, ProductMatch, ScrapeRun, SearchJob, SourceProduct
from src.scrapers.base import BaseScraper
from src.services.search_jobs import _job_completion_status, _job_warnings
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

    def order_by(self, *args, **kwargs):
        return self

    def count(self):
        return len(self.items)

    def options(self, *args, **kwargs):
        return self

    def limit(self, value):
        return _FakeQuery(self.items[:value])

    def filter(self, *conditions):
        filtered = []
        for item in self.items:
            keep = True
            for condition in conditions:
                attr_name = getattr(condition.left, "name", None)
                operator = getattr(condition.operator, "__name__", "")
                left_value = getattr(item, attr_name, None)
                if operator == "eq":
                    expected = getattr(condition.right, "value", condition.right)
                    if attr_name is None or left_value != expected:
                        keep = False
                        break
                elif operator == "in_op":
                    expected = list(condition.right.value)
                    if attr_name is None or left_value not in expected:
                        keep = False
                        break
                else:
                    raise AssertionError(f"Unsupported filter operator: {operator}")
            if keep:
                filtered.append(item)
        return _FakeQuery(filtered)


class _FakeSession:
    def __init__(self, canonicals):
        self.canonicals = canonicals
        self.added = []
        self.catalog_requests = []
        self.search_jobs = []
        self.commits = 0

    def query(self, model):
        if model is CanonicalProduct:
            return _FakeQuery(self.canonicals)
        if model is CatalogRequest:
            return _FakeQuery(self.catalog_requests)
        if model is SearchJob:
            return _FakeQuery(self.search_jobs)
        if model is ScrapeRun:
            return _FakeQuery(getattr(self, "scrape_runs", []))
        if model is SourceProduct:
            return _FakeQuery(getattr(self, "source_products", []))
        if model is ProductMatch:
            return _FakeQuery(getattr(self, "matches", []))
        raise AssertionError(f"Unexpected model query: {model}")

    def add(self, instance):
        self.added.append(instance)
        if isinstance(instance, CatalogRequest):
            instance.id = len(self.catalog_requests) + 1
            self.catalog_requests.append(instance)
        if isinstance(instance, SearchJob):
            instance.id = len(self.search_jobs) + 1
            self.search_jobs.append(instance)

    def flush(self):
        return None

    def commit(self):
        self.commits += 1

    def refresh(self, instance):
        return None

    def count(self):
        raise AssertionError("count() should be called on _FakeQuery, not session")


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

    def test_item_availability_summary_classifies_out_of_stock_only(self):
        summary = _item_availability_summary(
            {
                "match_found": True,
                "best_offer": None,
                "offers": [{"pharmacy": "Panvel", "price": 10.0, "availability": "out_of_stock"}],
            }
        )
        self.assertEqual(summary["state"], "only_out_of_stock_offers")
        self.assertEqual(summary["offer_counts"]["out_of_stock"], 1)

    def test_build_basket_result_includes_availability_summary(self):
        result = _build_basket_result(
            [
                {
                    "match_found": True,
                    "requested_item": "novalgina 1g",
                    "best_offer": {"pharmacy": "Drogasil", "price": 11.0, "availability": "available"},
                    "offers": [{"pharmacy": "Drogasil", "price": 11.0, "availability": "available"}],
                },
                {
                    "match_found": True,
                    "requested_item": "mounjaro 15mg",
                    "best_offer": None,
                    "offers": [{"pharmacy": "Drogaria Catarinense", "price": 3590.0, "availability": "out_of_stock"}],
                },
            ]
        )
        self.assertEqual(result["availability_summary"]["items_with_available_offers"], 1)
        self.assertEqual(result["availability_summary"]["items_only_out_of_stock_offers"], 1)

    def test_snapshot_freshness_payload_marks_recent_data_as_fresh(self):
        snapshot = PriceSnapshot(captured_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(minutes=30), scrape_run_id=7)
        freshness = _snapshot_freshness_payload(snapshot)
        self.assertEqual(freshness["freshness_status"], "fresh")
        self.assertEqual(freshness["scrape_run_id"], 7)

    def test_basket_freshness_summary_counts_states(self):
        summary = _basket_freshness_summary(
            [
                {
                    "best_offer": {
                        "data_freshness": {
                            "data_age_minutes": 30,
                            "freshness_status": "fresh",
                        }
                    }
                },
                {
                    "best_offer": {
                        "data_freshness": {
                            "data_age_minutes": 900,
                            "freshness_status": "stale",
                        }
                    }
                },
            ]
        )
        self.assertEqual(summary["fresh_items"], 1)
        self.assertEqual(summary["stale_items"], 1)
        self.assertEqual(summary["oldest_data_age_minutes"], 900)

    def test_freshness_status_marks_old_data_as_expired(self):
        self.assertEqual(_freshness_status(datetime.now(UTC).replace(tzinfo=None) - timedelta(days=2)), "expired")

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

    def test_register_catalog_request_upserts_by_query_and_cep(self):
        session = _FakeSession([])
        first = _register_catalog_request(session, "mounjaro 15mg", "89254300", "search_products")
        second = _register_catalog_request(session, "Mounjaro 15mg", "89254300", "compare_shopping_list")

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.request_count, 2)
        self.assertEqual(second.last_requested_by_tool, "compare_shopping_list")

    def test_register_search_job_upserts_active_job_by_query_and_cep(self):
        session = _FakeSession([])
        catalog_request = _register_catalog_request(session, "mounjaro 15mg", "89254300", "search_products")

        first = _register_search_job(session, "mounjaro 15mg", "89254300", "search_products", catalog_request)
        second = _register_search_job(session, "Mounjaro 15mg", "89254300", "compare_shopping_list", catalog_request)

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.request_count, 2)
        self.assertEqual(second.requested_by_tool, "compare_shopping_list")
        self.assertEqual(second.position_hint, 1)
        self.assertEqual(second.catalog_request_id, catalog_request.id)

    def test_search_job_payload_exposes_queue_status(self):
        session = _FakeSession([])
        job = _register_search_job(session, "produto raro xyz", "89254300", "search_products")

        payload = _search_job_payload(job, session)

        self.assertEqual(payload["status"], "queued")
        self.assertEqual(payload["position"], 1)
        self.assertEqual(payload["eta_seconds"], 0)

    def test_search_job_payload_exposes_warnings_from_result_payload(self):
        session = _FakeSession([])
        job = _register_search_job(session, "produto raro xyz", "89254300", "search_products")
        job.status = "partial_success"
        job.result_payload = {
            "warnings": [
                {
                    "code": "partial_scraper_failure",
                    "message": "Uma ou mais farmacias falharam durante a busca sob demanda.",
                    "pharmacies": ["panvel"],
                }
            ]
        }

        payload = _search_job_payload(job, session)

        self.assertEqual(payload["status"], "partial_success")
        self.assertEqual(payload["warnings"][0]["code"], "partial_scraper_failure")

    def test_job_completion_status_marks_partial_success(self):
        status = _job_completion_status(
            [
                {"pharmacy_slug": "a", "status": "completed"},
                {"pharmacy_slug": "b", "status": "skipped"},
            ]
        )
        self.assertEqual(status, "partial_success")

    def test_job_warnings_include_partial_failure_and_no_results(self):
        warnings = _job_warnings(
            [
                {"pharmacy_slug": "panvel", "status": "failed"},
                {"pharmacy_slug": "drogasil", "status": "completed"},
            ],
            {"results": []},
        )
        self.assertEqual(warnings[0]["code"], "partial_scraper_failure")
        self.assertEqual(warnings[1]["code"], "no_results_found")

    def test_job_warnings_include_runtime_unavailable_when_scraper_is_skipped(self):
        warnings = _job_warnings(
            [
                {"pharmacy_slug": "panvel", "status": "skipped"},
                {"pharmacy_slug": "drogasil", "status": "completed"},
            ],
            {"results": []},
        )
        self.assertEqual(warnings[0]["code"], "scraper_runtime_unavailable")

    def test_queue_metrics_counts_jobs_by_status(self):
        session = _FakeSession([])
        queued = _register_search_job(session, "produto a", "89254300", "search_products")
        processing = SearchJob(
            id=2,
            query="produto b",
            normalized_query="produto b",
            cep="89254300",
            status="processing",
            requested_by_tool="search_products",
            request_count=1,
            created_at=datetime.now(UTC).replace(tzinfo=None),
            updated_at=datetime.now(UTC).replace(tzinfo=None),
        )
        session.search_jobs.append(processing)

        metrics = _queue_metrics(session)

        self.assertEqual(metrics["queued_jobs"], 1)
        self.assertEqual(metrics["processing_jobs"], 1)
        self.assertEqual(metrics["total_jobs"], 2)
        self.assertIsNotNone(metrics["oldest_queued_job_minutes"])

    def test_pharmacy_metrics_summarize_matching_and_availability(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        source_product = SourceProduct(id=1, pharmacy=pharmacy, raw_name="Novalgina", normalized_name="novalgina", source_sku="1")
        source_product.match = ProductMatch(review_status="auto_approved", match_type="ean_gtin", confidence=1.0)
        session.source_products = [source_product]
        session.matches = [source_product.match]
        session.added = []
        session.catalog_requests = []
        session.scrape_runs = []
        source_product.prices = []

        original_builder = __import__("src.main", fromlist=["_build_latest_price_map"])._build_latest_price_map
        try:
            import src.main as main_module

            main_module._build_latest_price_map = lambda db: {
                1: PriceSnapshot(
                    source_product_id=1,
                    price=10.0,
                    availability="available",
                    captured_at=datetime.now(UTC).replace(tzinfo=None),
                    scrape_run_id=1,
                )
            }
            metrics = _pharmacy_metrics(session)
        finally:
            main_module._build_latest_price_map = original_builder

        self.assertEqual(metrics["Panvel"]["source_products"], 1)
        self.assertEqual(metrics["Panvel"]["auto_approved_matches"], 1)
        self.assertEqual(metrics["Panvel"]["availability_counts"]["available"], 1)

    def test_ops_health_marks_failed_runs_as_degraded(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        session.scrape_runs = [
            ScrapeRun(
                id=1,
                pharmacy=pharmacy,
                cep="89254300",
                trigger_type="scheduled",
                status="failed",
                search_terms=["novalgina"],
                products_seen=10,
                products_saved=0,
                error_count=1,
                started_at=datetime.now(UTC).replace(tzinfo=None),
            )
        ]

        payload = _ops_health_payload(session)

        self.assertEqual(payload["status"], "degraded")
        self.assertIn("Panvel", payload["failed_pharmacies"])


if __name__ == "__main__":
    unittest.main()
