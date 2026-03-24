import unittest
from datetime import UTC, datetime, timedelta

import src.scrapers.base as scraper_base_module
from src.mcp_server import _tool_definitions as _mcp_tool_definitions
from src.main import (
    _ops_health_payload,
    _pharmacy_metrics,
    list_canonical_products as _list_canonical_products,
    list_source_products as _list_source_products,
)
from src.models.base import CanonicalProduct
from src.models.base import CatalogRequest, OperationJob, Pharmacy, PriceSnapshot, ProductMatch, ScrapeRun, SearchJob, SourceProduct, TrackedItemByCep
from src.scrapers.base import BaseScraper
from src.services.catalog_queries import (
    anchor_search_tokens,
    availability_rank as _availability_rank,
    best_pricing_offer as _best_pricing_offer,
    build_latest_price_map as _build_latest_price_map,
    freshness_status as _freshness_status,
    has_special_token_conflict as _has_special_token_conflict,
    normalize_cep as _normalize_cep,
    normalize_query as _normalize_query,
    preferred_search_terms,
    score_canonical_match as _score_canonical_match,
    snapshot_freshness_payload as _snapshot_freshness_payload,
    tokenize_search_text as _tokenize_search_text,
    validate_cep_context as _validate_cep_context,
)
from src.services.demand_tracking import (
    queue_metrics as _queue_metrics,
    register_catalog_request as _register_catalog_request,
    register_search_job as _register_search_job,
    register_tracked_item as _register_tracked_item,
    search_job_payload as _search_job_payload,
    tracked_item_priority as _tracked_item_priority,
    tracked_item_status as _tracked_item_status,
)
from src.services.external_health import (
    page_health_payload as _page_health_payload,
    scraper_health_payload as _scraper_health_payload,
)
from src.services.operation_jobs import (
    JOB_TYPE_SCHEDULED_COLLECTION,
    enqueue_operation_job as _enqueue_operation_job,
    list_operation_jobs as _list_operation_jobs,
    operation_job_metrics as _operation_job_metrics,
    operation_job_payload as _operation_job_payload,
    process_operation_job as _process_operation_job,
)
from src.services.search_jobs import _job_completion_status, _job_warnings
from src.services.operational_cycle import collection_schedule_status, parse_collection_slots
from src.services.ops import live_health_payload as _live_health_payload
from src.services.ops import ops_metrics_payload as _ops_metrics_payload
from src.services.ops import readiness_health_payload as _readiness_health_payload
from src.services.retention import purge_expired_operational_data_in_session as _purge_expired_operational_data_in_session
from src.services.scheduled_collection import (
    _collection_run_status,
    _collection_search_term,
    _group_tracked_items_for_plan,
    _should_mark_items_scraped,
    _tracked_item_status_for_scheduler,
)
from src.services.matching import ProductMatcher
from src.services.tool_models import ObservedItemRequest
from src.services.tool_use import (
    availability_warnings as _availability_warnings,
    basket_availability_summary as _basket_availability_summary,
    basket_freshness_summary as _basket_freshness_summary,
    build_basket_result as _build_basket_result,
    build_observed_query as _build_observed_query,
    build_price_summary as _build_price_summary,
    compare_canonical_product_service as _compare_canonical_product_service,
    estimate_overall_confidence as _estimate_overall_confidence,
    item_availability_summary as _item_availability_summary,
    search_products_service as _search_products_service,
    tool_response as _tool_response,
)


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

    def offset(self, value):
        return _FakeQuery(self.items[value:])

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
        self.canonicals = list(canonicals)
        self.added = []
        self.pharmacies = []
        self.scrape_runs = []
        self.source_products = []
        self.matches = []
        self.price_snapshots = []
        self.catalog_requests = []
        self.operation_jobs = []
        self.search_jobs = []
        self.tracked_items = []
        self.commits = 0
        self.closed = False
        self.flush_validator = None

    def query(self, model):
        if model is CanonicalProduct:
            return _FakeQuery(self.canonicals)
        if model is Pharmacy:
            return _FakeQuery(self.pharmacies)
        if model is CatalogRequest:
            return _FakeQuery(self.catalog_requests)
        if model is OperationJob:
            return _FakeQuery(self.operation_jobs)
        if model is SearchJob:
            return _FakeQuery(self.search_jobs)
        if model is TrackedItemByCep:
            return _FakeQuery(self.tracked_items)
        if model is ScrapeRun:
            return _FakeQuery(getattr(self, "scrape_runs", []))
        if model is SourceProduct:
            return _FakeQuery(self.source_products)
        if model is ProductMatch:
            return _FakeQuery(self.matches)
        if model is PriceSnapshot:
            return _FakeQuery(self.price_snapshots)
        raise AssertionError(f"Unexpected model query: {model}")

    def add(self, instance):
        self.added.append(instance)
        if isinstance(instance, CanonicalProduct):
            if not getattr(instance, "id", None):
                instance.id = len(self.canonicals) + 1
            if instance not in self.canonicals:
                self.canonicals.append(instance)
        if isinstance(instance, Pharmacy):
            if not getattr(instance, "id", None):
                instance.id = len(self.pharmacies) + 1
            if instance not in self.pharmacies:
                self.pharmacies.append(instance)
        if isinstance(instance, ScrapeRun):
            instance.id = instance.id or len(self.scrape_runs) + 1
            self.scrape_runs.append(instance)
        if isinstance(instance, SourceProduct):
            instance.id = instance.id or len(self.source_products) + 1
            self.source_products.append(instance)
        if isinstance(instance, ProductMatch):
            instance.id = instance.id or len(self.matches) + 1
            self.matches.append(instance)
            source_product = self.get(SourceProduct, instance.source_product_id)
            if source_product:
                source_product.match = instance
                instance.source_product = source_product
        if isinstance(instance, CatalogRequest):
            instance.id = len(self.catalog_requests) + 1
            self.catalog_requests.append(instance)
        if isinstance(instance, OperationJob):
            instance.id = len(self.operation_jobs) + 1
            self.operation_jobs.append(instance)
        if isinstance(instance, SearchJob):
            instance.id = len(self.search_jobs) + 1
            self.search_jobs.append(instance)
        if isinstance(instance, TrackedItemByCep):
            instance.id = len(self.tracked_items) + 1
            self.tracked_items.append(instance)
        if isinstance(instance, PriceSnapshot):
            instance.id = instance.id or len(self.price_snapshots) + 1
            self.price_snapshots.append(instance)

    def flush(self):
        if self.flush_validator:
            self.flush_validator(self)
        return None

    def commit(self):
        self.commits += 1

    def rollback(self):
        return None

    def refresh(self, instance):
        return None

    def get(self, model, primary_key):
        items = {
            CanonicalProduct: self.canonicals,
            Pharmacy: self.pharmacies,
            ScrapeRun: self.scrape_runs,
            SourceProduct: self.source_products,
            ProductMatch: self.matches,
            PriceSnapshot: self.price_snapshots,
        }.get(model)
        if items is None:
            raise AssertionError(f"Unexpected model get: {model}")
        for item in items:
            if getattr(item, "id", None) == primary_key:
                return item
        return None

    def execute(self, statement):
        self.last_statement = str(statement)
        return 1

    def delete(self, instance):
        if isinstance(instance, PriceSnapshot) and instance in self.price_snapshots:
            self.price_snapshots.remove(instance)
        if isinstance(instance, ScrapeRun) and instance in self.scrape_runs:
            self.scrape_runs.remove(instance)
        if isinstance(instance, CatalogRequest) and instance in self.catalog_requests:
            self.catalog_requests.remove(instance)
        if isinstance(instance, OperationJob) and instance in self.operation_jobs:
            self.operation_jobs.remove(instance)
        if isinstance(instance, SearchJob) and instance in self.search_jobs:
            self.search_jobs.remove(instance)
        if isinstance(instance, TrackedItemByCep) and instance in self.tracked_items:
            self.tracked_items.remove(instance)

    def count(self):
        raise AssertionError("count() should be called on _FakeQuery, not session")

    def close(self):
        self.closed = True


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

    def test_anchor_search_tokens_focuses_on_identity_terms(self):
        anchors = anchor_search_tokens("jardiance empagliflozina 25 mg 30 comprimidos revestidos")
        self.assertIn("jardiance", anchors)
        self.assertIn("empagliflozina", anchors)
        self.assertNotIn("comprimidos", anchors)
        self.assertNotIn("revestidos", anchors)

    def test_preferred_search_terms_reduce_verbose_ocr_query(self):
        terms = preferred_search_terms("jardiance empagliflozina 25 mg uso oral uso adulto 30 comprimidos revestidos")
        self.assertEqual(terms[0], "jardiance 25mg")
        self.assertIn("jardiance", terms)

    def test_preferred_search_terms_do_not_promote_volume_only_as_primary_term(self):
        terms = preferred_search_terms("clonazepam gotas 20ml ems")
        self.assertEqual(terms[0], "clonazepam")

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

    def test_validate_cep_context_accepts_any_valid_cep(self):
        self.assertEqual(_validate_cep_context("01.234-567"), "01234567")

    def test_mcp_tool_definitions_hide_admin_tools_by_default(self):
        tool_names = {tool["name"] for tool in _mcp_tool_definitions()}
        self.assertNotIn("list_review_matches", tool_names)
        self.assertNotIn("list_search_jobs", tool_names)
        self.assertIn("get_search_job", tool_names)

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

    def test_build_latest_price_map_keeps_newest_snapshot_per_source_product(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        session = _FakeSession([])
        session.price_snapshots = [
            PriceSnapshot(id=1, source_product_id=10, price=18.0, captured_at=now - timedelta(hours=3), scrape_run_id=1),
            PriceSnapshot(id=2, source_product_id=11, price=25.0, captured_at=now - timedelta(hours=1), scrape_run_id=2),
            PriceSnapshot(id=3, source_product_id=10, price=17.5, captured_at=now - timedelta(minutes=20), scrape_run_id=3),
        ]

        latest = _build_latest_price_map(session)

        self.assertEqual(latest[10].id, 3)
        self.assertEqual(latest[11].id, 2)

    def test_build_latest_price_map_filters_by_cep(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        session = _FakeSession([])
        session.price_snapshots = [
            PriceSnapshot(id=1, source_product_id=10, price=18.0, captured_at=now - timedelta(hours=2), scrape_run_id=1, cep="89254300"),
            PriceSnapshot(id=2, source_product_id=10, price=16.5, captured_at=now - timedelta(hours=1), scrape_run_id=2, cep="01001000"),
            PriceSnapshot(id=3, source_product_id=11, price=25.0, captured_at=now - timedelta(minutes=30), scrape_run_id=3, cep="89254300"),
        ]

        latest = _build_latest_price_map(session, "89254300")

        self.assertEqual(set(latest.keys()), {10, 11})
        self.assertEqual(latest[10].id, 1)
        self.assertEqual(latest[11].id, 3)

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

    def test_base_scraper_build_probe_specs_uses_declared_probe_contract(self):
        class _ProbeScraper(BaseScraper):
            pharmacy_slug = "farmacia-teste"
            runtime_type = "browser"
            search_probe_format = "{base_domain}/busca?term={encoded_term}"
            search_probe_response_type = "html"
            search_probe_expected_content_type = "text/html"
            search_probe_contains_term = True

            def __init__(self):
                super().__init__("https://example.com")
                self.base_domain = "https://example.com"

        scraper = _ProbeScraper()
        specs = scraper.build_probe_specs("dipirona sodica")

        self.assertEqual(specs[0]["probe_name"], "homepage")
        self.assertEqual(specs[1]["url"], "https://example.com/busca?term=dipirona+sodica")
        self.assertEqual(specs[1]["contains_any"], ["dipirona sodica"])

    def test_base_scraper_save_products_to_db_persists_product_match_and_snapshot(self):
        class _PersistScraper(BaseScraper):
            pharmacy_slug = "farmacia-teste"

            def __init__(self):
                super().__init__("https://example.com")
                self.search_terms = ["novalgina"]

        session = _FakeSession([])
        session.pharmacies = [Pharmacy(id=1, name="Farmacia Teste", slug="farmacia-teste")]
        original_session_local = scraper_base_module.SessionLocal
        try:
            scraper_base_module.SessionLocal = lambda: session
            scraper = _PersistScraper()
            scraper.save_products_to_db(
                [
                    {
                        "source_sku": "sku-1",
                        "source_url": "https://example.com/p/1",
                        "raw_name": "Novalgina 1g 10 Comprimidos",
                        "normalized_name": "novalgina 1g 10 comprimidos",
                        "brand": "Novalgina",
                        "dosage": "1g",
                        "presentation": "comprimido",
                        "pack_size": "10 comprimidos",
                        "price": 19.9,
                        "availability": "available",
                    }
                ]
            )
        finally:
            scraper_base_module.SessionLocal = original_session_local

        self.assertTrue(session.closed)
        self.assertEqual(len(session.source_products), 1)
        self.assertEqual(len(session.matches), 1)
        self.assertEqual(len(session.price_snapshots), 1)
        self.assertEqual(session.scrape_runs[0].status, "completed")
        self.assertEqual(session.scrape_runs[0].products_seen, 1)
        self.assertEqual(session.scrape_runs[0].products_saved, 1)

    def test_base_scraper_save_products_to_db_skips_incomplete_records_when_required(self):
        class _PersistScraper(BaseScraper):
            pharmacy_slug = "farmacia-teste"

            def __init__(self):
                super().__init__("https://example.com")
                self.search_terms = ["novalgina"]

        session = _FakeSession([])
        session.pharmacies = [Pharmacy(id=1, name="Farmacia Teste", slug="farmacia-teste")]
        original_session_local = scraper_base_module.SessionLocal
        try:
            scraper_base_module.SessionLocal = lambda: session
            scraper = _PersistScraper()
            scraper.save_products_to_db(
                [
                    {
                        "raw_name": "Registro incompleto",
                        "normalized_name": "registro incompleto",
                        "price": 19.9,
                    },
                    {
                        "source_sku": "sku-2",
                        "source_url": "https://example.com/p/2",
                        "raw_name": "Novalgina 500mg 20 Comprimidos",
                        "normalized_name": "novalgina 500mg 20 comprimidos",
                        "brand": "Novalgina",
                        "dosage": "500mg",
                        "presentation": "comprimido",
                        "pack_size": "20 comprimidos",
                        "price": 12.5,
                    },
                ],
                required_fields=("source_sku", "price"),
            )
        finally:
            scraper_base_module.SessionLocal = original_session_local

        self.assertEqual(len(session.source_products), 1)
        self.assertEqual(len(session.matches), 1)
        self.assertEqual(len(session.price_snapshots), 1)
        self.assertEqual(session.scrape_runs[0].products_seen, 2)
        self.assertEqual(session.scrape_runs[0].products_saved, 1)

    def test_base_scraper_save_products_to_db_derives_raw_name_from_alternate_fields(self):
        class _PersistScraper(BaseScraper):
            pharmacy_slug = "farmacia-teste"

            def __init__(self):
                super().__init__("https://example.com")
                self.search_terms = ["dipirona"]

        session = _FakeSession([])
        session.pharmacies = [Pharmacy(id=1, name="Farmacia Teste", slug="farmacia-teste")]
        original_session_local = scraper_base_module.SessionLocal
        try:
            scraper_base_module.SessionLocal = lambda: session
            scraper = _PersistScraper()
            scraper.save_products_to_db(
                [
                    {
                        "sku": "sku-3",
                        "title": "Dipirona 1g 10 Comprimidos",
                        "link": "https://example.com/p/3",
                        "price": 9.9,
                        "availability": "available",
                    }
                ]
            )
        finally:
            scraper_base_module.SessionLocal = original_session_local

        self.assertEqual(len(session.source_products), 1)
        self.assertEqual(session.source_products[0].raw_name, "Dipirona 1g 10 Comprimidos")
        self.assertEqual(session.source_products[0].normalized_name, "dipirona 1g 10 comprimidos")
        self.assertEqual(session.source_products[0].source_sku, "sku-3")
        self.assertEqual(len(session.price_snapshots), 1)

    def test_base_scraper_flushes_only_after_populating_required_source_product_fields(self):
        class _PersistScraper(BaseScraper):
            pharmacy_slug = "farmacia-teste"

            def __init__(self):
                super().__init__("https://example.com")
                self.search_terms = ["buscopan"]

        session = _FakeSession([])
        session.pharmacies = [Pharmacy(id=1, name="Farmacia Teste", slug="farmacia-teste")]

        def _assert_source_products_ready_before_flush(active_session):
            for source_product in active_session.source_products:
                self.assertTrue(source_product.source_sku)
                self.assertTrue(source_product.raw_name)
                self.assertTrue(source_product.normalized_name)

        session.flush_validator = _assert_source_products_ready_before_flush
        original_session_local = scraper_base_module.SessionLocal
        try:
            scraper_base_module.SessionLocal = lambda: session
            scraper = _PersistScraper()
            scraper.save_products_to_db(
                [
                    {
                        "sku": "sku-4",
                        "title": "Buscopan 10mg 20 Drageas",
                        "link": "https://example.com/p/4",
                        "price": 14.9,
                    }
                ]
            )
        finally:
            scraper_base_module.SessionLocal = original_session_local

        self.assertEqual(len(session.source_products), 1)
        self.assertEqual(session.source_products[0].raw_name, "Buscopan 10mg 20 Drageas")

    def test_list_source_products_applies_limit_and_offset(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        first = SourceProduct(id=1, pharmacy=pharmacy, raw_name="Produto A", normalized_name="produto a", source_sku="a")
        second = SourceProduct(id=2, pharmacy=pharmacy, raw_name="Produto B", normalized_name="produto b", source_sku="b")
        session.source_products = [first, second]

        payload = _list_source_products(limit=1, offset=1, db=session)

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], 2)
        self.assertEqual(payload[0]["pharmacy"], "Panvel")

    def test_list_source_products_can_filter_by_cep(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        first = SourceProduct(id=1, pharmacy=pharmacy, raw_name="Produto A", normalized_name="produto a", source_sku="a")
        second = SourceProduct(id=2, pharmacy=pharmacy, raw_name="Produto B", normalized_name="produto b", source_sku="b")
        session.source_products = [first, second]

        import src.main as main_module

        original_builder = main_module.build_latest_price_map
        try:
            main_module.build_latest_price_map = lambda db, cep=None: {1: PriceSnapshot(source_product_id=1, price=10.0, captured_at=datetime.now(UTC).replace(tzinfo=None), scrape_run_id=1, cep=cep)}
            payload = _list_source_products(cep="89254300", limit=10, offset=0, db=session)
        finally:
            main_module.build_latest_price_map = original_builder

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], 1)

    def test_list_canonical_products_applies_limit_and_offset(self):
        session = _FakeSession(
            [
                CanonicalProduct(id=1, canonical_name="Produto A", normalized_name="produto a"),
                CanonicalProduct(id=2, canonical_name="Produto B", normalized_name="produto b"),
            ]
        )

        payload = _list_canonical_products(limit=1, offset=1, db=session)

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], 2)
        self.assertEqual(payload[0]["canonical_name"], "Produto B")

    def test_list_canonical_products_can_filter_by_cep(self):
        canonical_a = CanonicalProduct(id=1, canonical_name="Produto A", normalized_name="produto a")
        canonical_b = CanonicalProduct(id=2, canonical_name="Produto B", normalized_name="produto b")
        session = _FakeSession([canonical_a, canonical_b])
        session.matches = [
            ProductMatch(id=1, source_product_id=10, canonical_product_id=1),
            ProductMatch(id=2, source_product_id=20, canonical_product_id=2),
        ]

        import src.main as main_module

        original_builder = main_module.build_latest_price_map
        try:
            main_module.build_latest_price_map = lambda db, cep=None: {10: PriceSnapshot(source_product_id=10, price=10.0, captured_at=datetime.now(UTC).replace(tzinfo=None), scrape_run_id=1, cep=cep)}
            payload = _list_canonical_products(cep="89254300", limit=10, offset=0, db=session)
        finally:
            main_module.build_latest_price_map = original_builder

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], 1)

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

    def test_score_canonical_match_rejects_different_medicine_name(self):
        canonical = CanonicalProduct(
            canonical_name="Novalgina 500mg 30 Comprimidos",
            normalized_name="novalgina 500mg 30 comprimidos",
            brand="Novalgina",
            dosage="500mg",
            pack_size="30 comprimidos",
        )
        self.assertEqual(_score_canonical_match(canonical, "oxcarbazepina 600mg 30 comprimidos"), 0)
        self.assertEqual(_score_canonical_match(canonical, "jardiance empagliflozina 25mg 30 comprimidos revestidos"), 0)

    def test_register_catalog_request_upserts_by_query_and_cep(self):
        session = _FakeSession([])
        first = _register_catalog_request(session, "mounjaro 15mg", "89254300", "search_products")
        second = _register_catalog_request(session, "Mounjaro 15mg", "89254300", "compare_shopping_list")

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.request_count, 2)
        self.assertEqual(second.last_requested_by_tool, "compare_shopping_list")

    def test_tracked_item_status_changes_with_age(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        self.assertEqual(_tracked_item_status(now), "active")
        self.assertEqual(_tracked_item_status(now - timedelta(days=45)), "cooldown")
        self.assertEqual(_tracked_item_status(now - timedelta(days=120)), "inactive")

    def test_tracked_item_priority_favors_recent_and_canonical_items(self):
        now = datetime.now(UTC).replace(tzinfo=None)
        high_priority = _tracked_item_priority(5, now, 10)
        low_priority = _tracked_item_priority(1, now - timedelta(days=100), None)
        self.assertGreater(high_priority, low_priority)

    def test_register_tracked_item_upserts_by_query_and_cep(self):
        session = _FakeSession([])
        first = _register_tracked_item(session, "mounjaro 15mg", "89254300", "search_products")
        second = _register_tracked_item(session, "Mounjaro 15mg", "89254300", "compare_shopping_list")

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.request_count_total, 2)
        self.assertEqual(second.status, "active")
        self.assertEqual(second.last_requested_by_tool, "compare_shopping_list")

    def test_register_tracked_item_merges_query_into_existing_canonical_item(self):
        session = _FakeSession([])
        canonical = CanonicalProduct(id=10, canonical_name="Mounjaro 15mg", normalized_name="mounjaro 15mg")
        tracked_by_query = TrackedItemByCep(
            id=1,
            cep="89254300",
            query="mounjaro 15mg",
            normalized_query="mounjaro 15mg",
            request_count_total=1,
            first_requested_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(days=2),
            last_requested_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(days=1),
            status="active",
            scrape_priority=100,
        )
        tracked_by_canonical = TrackedItemByCep(
            id=2,
            cep="89254300",
            query="tirzepatida 15mg",
            normalized_query="tirzepatida 15mg",
            canonical_product_id=10,
            request_count_total=3,
            first_requested_at=datetime.now(UTC).replace(tzinfo=None) - timedelta(days=5),
            last_requested_at=datetime.now(UTC).replace(tzinfo=None),
            status="active",
            scrape_priority=110,
        )
        session.tracked_items = [tracked_by_query, tracked_by_canonical]

        merged = _register_tracked_item(
            session,
            "mounjaro 15mg",
            "89254300",
            "search_products",
            canonical_product=canonical,
        )

        self.assertEqual(merged.id, 2)
        self.assertEqual(len(session.tracked_items), 1)
        self.assertEqual(merged.request_count_total, 5)
        self.assertEqual(merged.canonical_product_id, 10)

    def test_scheduler_status_marks_old_item_inactive(self):
        self.assertEqual(
            _tracked_item_status_for_scheduler(datetime.now(UTC).replace(tzinfo=None) - timedelta(days=120)),
            "inactive",
        )

    def test_group_tracked_items_for_plan_limits_per_cep_and_excludes_inactive(self):
        items = [
            TrackedItemByCep(
                id=1,
                cep="89254300",
                query="item a",
                normalized_query="item a",
                status="active",
                scrape_priority=100,
            ),
            TrackedItemByCep(
                id=2,
                cep="89254300",
                query="item b",
                normalized_query="item b",
                status="cooldown",
                scrape_priority=90,
            ),
            TrackedItemByCep(
                id=3,
                cep="89254300",
                query="item c",
                normalized_query="item c",
                status="inactive",
                scrape_priority=1000,
            ),
        ]

        grouped = _group_tracked_items_for_plan(items, include_cooldown=True, limit_per_cep=2)

        self.assertEqual(len(grouped["89254300"]), 2)
        self.assertEqual([item.id for item in grouped["89254300"]], [1, 2])

    def test_collection_search_term_prefers_short_stable_term(self):
        item = TrackedItemByCep(
            cep="89254300",
            query="Novalgina 1g 10 comprimidos",
            normalized_query="novalgina 1g 10 comprimidos",
            canonical_product_id=None,
            status="active",
            scrape_priority=100,
        )
        self.assertEqual(_collection_search_term(item), "novalgina")

    def test_collection_run_status_marks_partial_success_when_any_scraper_completes(self):
        status = _collection_run_status(
            [
                {"pharmacy_slug": "a", "status": "completed"},
                {"pharmacy_slug": "b", "status": "failed"},
            ]
        )
        self.assertEqual(status, "partial_success")

    def test_collection_run_status_marks_failed_when_no_scraper_succeeds(self):
        status = _collection_run_status(
            [
                {"pharmacy_slug": "a", "status": "failed"},
                {"pharmacy_slug": "b", "status": "skipped"},
            ]
        )
        self.assertEqual(status, "failed")

    def test_should_mark_items_scraped_only_after_successful_scraper(self):
        self.assertTrue(_should_mark_items_scraped([{"status": "completed"}, {"status": "failed"}]))
        self.assertFalse(_should_mark_items_scraped([{"status": "failed"}, {"status": "skipped"}]))

    def test_enqueue_operation_job_reuses_active_duplicate(self):
        session = _FakeSession([])
        first = _enqueue_operation_job(
            session,
            job_type=JOB_TYPE_SCHEDULED_COLLECTION,
            requested_by="ops_api",
            payload={"cep": "89254300"},
        )
        second = _enqueue_operation_job(
            session,
            job_type=JOB_TYPE_SCHEDULED_COLLECTION,
            requested_by="ops_api",
            payload={"cep": "89254300"},
        )

        self.assertEqual(first.id, second.id)
        self.assertEqual(second.request_count, 2)

    def test_operation_job_payload_exposes_queue_fields(self):
        job = OperationJob(
            id=7,
            job_type=JOB_TYPE_SCHEDULED_COLLECTION,
            requested_by="ops_api",
            status="queued",
            request_count=1,
            payload={"cep": "89254300"},
        )
        payload = _operation_job_payload(job)

        self.assertEqual(payload["operation_job_id"], 7)
        self.assertEqual(payload["job_type"], JOB_TYPE_SCHEDULED_COLLECTION)
        self.assertEqual(payload["status"], "queued")

    def test_list_operation_jobs_applies_limit_and_offset(self):
        session = _FakeSession([])
        session.operation_jobs = [
            OperationJob(id=1, job_type="a", requested_by="ops_api", status="queued"),
            OperationJob(id=2, job_type="b", requested_by="ops_api", status="processing"),
        ]

        payload = _list_operation_jobs(session, limit=1, offset=1)

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["operation_job_id"], 2)

    def test_list_operation_jobs_can_filter_by_cep(self):
        session = _FakeSession([])
        session.operation_jobs = [
            OperationJob(id=1, job_type="a", requested_by="ops_api", status="queued", payload={"cep": "89254300"}),
            OperationJob(id=2, job_type="b", requested_by="ops_api", status="processing", payload={"cep": "01001000"}),
        ]

        payload = _list_operation_jobs(session, limit=10, offset=0, cep="89254300")

        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["operation_job_id"], 1)

    def test_operation_job_metrics_count_statuses(self):
        session = _FakeSession([])
        session.operation_jobs = [
            OperationJob(id=1, job_type="a", requested_by="ops_api", status="queued", created_at=datetime.now(UTC).replace(tzinfo=None)),
            OperationJob(id=2, job_type="b", requested_by="ops_api", status="processing", created_at=datetime.now(UTC).replace(tzinfo=None)),
            OperationJob(id=3, job_type="c", requested_by="ops_api", status="failed", created_at=datetime.now(UTC).replace(tzinfo=None)),
        ]

        metrics = _operation_job_metrics(session)

        self.assertEqual(metrics["queued_jobs"], 1)
        self.assertEqual(metrics["processing_jobs"], 1)
        self.assertEqual(metrics["failed_jobs"], 1)

    def test_operation_job_metrics_can_scope_by_cep(self):
        session = _FakeSession([])
        session.operation_jobs = [
            OperationJob(id=1, job_type="a", requested_by="ops_api", status="queued", created_at=datetime.now(UTC).replace(tzinfo=None), payload={"cep": "89254300"}),
            OperationJob(id=2, job_type="b", requested_by="ops_api", status="processing", created_at=datetime.now(UTC).replace(tzinfo=None), payload={"cep": "01001000"}),
        ]

        metrics = _operation_job_metrics(session, "89254300")

        self.assertEqual(metrics["total_jobs"], 1)
        self.assertEqual(metrics["queued_jobs"], 1)
        self.assertEqual(metrics["processing_jobs"], 0)

    def test_process_operation_job_executes_custom_executor_and_completes(self):
        session = _FakeSession([])
        job = _enqueue_operation_job(
            session,
            job_type=JOB_TYPE_SCHEDULED_COLLECTION,
            requested_by="ops_api",
            payload={"cep": "89254300"},
        )

        class _SessionFactory:
            def __call__(self):
                return self

            def __enter__(self):
                return session

            def __exit__(self, exc_type, exc, tb):
                return False

        processed = _process_operation_job(
            job.id,
            session_factory=_SessionFactory(),
            executors={JOB_TYPE_SCHEDULED_COLLECTION: lambda payload: {"status": "completed", "payload": payload}},
        )

        self.assertEqual(processed.status, "completed")
        self.assertEqual(processed.result_payload["payload"]["cep"], "89254300")

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

    def test_search_products_service_enqueues_operation_job_on_first_miss(self):
        session = _FakeSession([])

        response = _search_products_service("produto raro xyz", "89254300", session)

        self.assertEqual(response["result"]["search_job"]["status"], "queued")
        self.assertEqual(response["result"]["operation_job"]["job_type"], "process_search_job")
        self.assertEqual(response["result"]["operation_job"]["status"], "queued")
        self.assertEqual(response["result"]["operation_job"]["payload"]["search_job_id"], response["result"]["search_job"]["job_id"])
        self.assertEqual(len(session.operation_jobs), 1)

    def test_compare_canonical_product_service_filters_offers_by_cep(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        source_product = SourceProduct(id=1, pharmacy_id=1, pharmacy=pharmacy, raw_name="Jardiance 25mg", normalized_name="jardiance 25mg", source_sku="1")
        canonical = CanonicalProduct(id=10, canonical_name="Jardiance 25mg", normalized_name="jardiance 25mg")
        match = ProductMatch(id=1, source_product_id=1, canonical_product_id=10, review_status="auto_approved", confidence=1.0)
        match.source_product = source_product
        match.canonical_product = canonical
        source_product.match = match
        canonical.matches = [match]
        session.pharmacies = [pharmacy]
        session.source_products = [source_product]
        session.matches = [match]
        session.canonicals = [canonical]
        now = datetime.now(UTC).replace(tzinfo=None)
        session.price_snapshots = [
            PriceSnapshot(id=1, source_product_id=1, price=199.0, availability="available", captured_at=now - timedelta(minutes=20), scrape_run_id=1, cep="89254300"),
            PriceSnapshot(id=2, source_product_id=1, price=149.0, availability="available", captured_at=now - timedelta(minutes=10), scrape_run_id=2, cep="01001000"),
        ]

        result = _compare_canonical_product_service(10, "89254300", session)

        self.assertEqual(result["cep"], "89254300")
        self.assertEqual(len(result["offers"]), 1)
        self.assertEqual(result["offers"][0]["price"], 199.0)

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

    def test_queue_metrics_can_scope_by_cep(self):
        session = _FakeSession([])
        _register_search_job(session, "produto a", "89254300", "search_products")
        _register_search_job(session, "produto b", "01001000", "search_products")

        metrics = _queue_metrics(session, "89254300")

        self.assertEqual(metrics["total_jobs"], 1)
        self.assertEqual(metrics["queued_jobs"], 1)

    def test_retention_purges_expired_operational_data_after_90_days(self):
        session = _FakeSession([])
        now = datetime.now(UTC).replace(tzinfo=None)
        old = now - timedelta(days=91)
        recent = now - timedelta(days=10)

        session.price_snapshots = [
            PriceSnapshot(id=1, source_product_id=10, price=18.0, captured_at=old, scrape_run_id=1, cep="89254300"),
            PriceSnapshot(id=2, source_product_id=11, price=17.5, captured_at=recent, scrape_run_id=2, cep="89254300"),
        ]
        session.scrape_runs = [
            ScrapeRun(id=1, pharmacy_id=1, cep="89254300", status="completed", started_at=old, finished_at=old),
            ScrapeRun(id=2, pharmacy_id=1, cep="89254300", status="completed", started_at=recent, finished_at=recent),
        ]
        old_request = CatalogRequest(
            id=1,
            query="produto antigo",
            normalized_query="produto antigo",
            cep="89254300",
            status="fulfilled",
            first_requested_at=old,
            last_requested_at=old,
        )
        recent_request = CatalogRequest(
            id=2,
            query="produto recente",
            normalized_query="produto recente",
            cep="89254300",
            status="pending",
            first_requested_at=recent,
            last_requested_at=recent,
        )
        session.catalog_requests = [old_request, recent_request]
        session.search_jobs = [
            SearchJob(
                id=1,
                query="produto antigo",
                normalized_query="produto antigo",
                cep="89254300",
                status="completed",
                requested_by_tool="search_products",
                request_count=1,
                created_at=old,
                updated_at=old,
                finished_at=old,
                catalog_request_id=1,
            ),
            SearchJob(
                id=2,
                query="produto recente",
                normalized_query="produto recente",
                cep="89254300",
                status="queued",
                requested_by_tool="search_products",
                request_count=1,
                created_at=recent,
                updated_at=recent,
                catalog_request_id=2,
            ),
        ]
        session.operation_jobs = [
            OperationJob(id=1, job_type="process_search_job", requested_by="tool", status="completed", created_at=old, updated_at=old, finished_at=old),
            OperationJob(id=2, job_type="process_search_job", requested_by="tool", status="queued", created_at=recent, updated_at=recent),
        ]
        session.tracked_items = [
            TrackedItemByCep(
                id=1,
                cep="89254300",
                query="produto antigo",
                normalized_query="produto antigo",
                status="inactive",
                request_count_total=1,
                scrape_priority=0,
                first_requested_at=old,
                last_requested_at=old,
            ),
            TrackedItemByCep(
                id=2,
                cep="89254300",
                query="produto recente",
                normalized_query="produto recente",
                status="active",
                request_count_total=1,
                scrape_priority=100,
                first_requested_at=recent,
                last_requested_at=recent,
            ),
        ]

        retention = _purge_expired_operational_data_in_session(session, retention_days=90, now=now)

        self.assertEqual(retention["deleted_snapshots"], 1)
        self.assertEqual(retention["deleted_scrape_runs"], 1)
        self.assertEqual(retention["deleted_search_jobs"], 1)
        self.assertEqual(retention["deleted_operation_jobs"], 1)
        self.assertEqual(retention["deleted_catalog_requests"], 1)
        self.assertEqual(retention["deleted_tracked_items"], 1)
        self.assertEqual(len(session.price_snapshots), 1)
        self.assertEqual(len(session.scrape_runs), 1)
        self.assertEqual(len(session.search_jobs), 1)
        self.assertEqual(len(session.operation_jobs), 1)
        self.assertEqual(len(session.catalog_requests), 1)
        self.assertEqual(len(session.tracked_items), 1)

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

        from src.services import ops as ops_module

        original_builder = ops_module.build_latest_price_map
        try:
            ops_module.build_latest_price_map = lambda db, cep=None: {
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
            ops_module.build_latest_price_map = original_builder

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

    def test_ops_metrics_payload_scopes_counts_by_cep(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        source_a = SourceProduct(id=1, pharmacy=pharmacy, pharmacy_id=1, raw_name="Produto A", normalized_name="produto a", source_sku="1")
        source_b = SourceProduct(id=2, pharmacy=pharmacy, pharmacy_id=1, raw_name="Produto B", normalized_name="produto b", source_sku="2")
        match_a = ProductMatch(id=1, source_product_id=1, canonical_product_id=10, match_type="ean_gtin", review_status="auto_approved", confidence=1.0)
        match_b = ProductMatch(id=2, source_product_id=2, canonical_product_id=20, match_type="manual_review", review_status="needs_review", confidence=0.5)
        source_a.match = match_a
        source_b.match = match_b
        session.source_products = [source_a, source_b]
        session.matches = [match_a, match_b]
        session.catalog_requests = [
            CatalogRequest(id=1, query="produto a", normalized_query="produto a", cep="89254300", status="pending"),
            CatalogRequest(id=2, query="produto b", normalized_query="produto b", cep="01001000", status="pending"),
        ]
        session.tracked_items = [
            TrackedItemByCep(id=1, cep="89254300", query="produto a", normalized_query="produto a", status="active", request_count_total=1, scrape_priority=100),
            TrackedItemByCep(id=2, cep="01001000", query="produto b", normalized_query="produto b", status="active", request_count_total=1, scrape_priority=100),
        ]
        session.search_jobs = [
            SearchJob(id=1, query="produto a", normalized_query="produto a", cep="89254300", status="queued", requested_by_tool="search_products", request_count=1, created_at=datetime.now(UTC).replace(tzinfo=None), updated_at=datetime.now(UTC).replace(tzinfo=None)),
            SearchJob(id=2, query="produto b", normalized_query="produto b", cep="01001000", status="queued", requested_by_tool="search_products", request_count=1, created_at=datetime.now(UTC).replace(tzinfo=None), updated_at=datetime.now(UTC).replace(tzinfo=None)),
        ]
        session.operation_jobs = [
            OperationJob(id=1, job_type="process_search_job", requested_by="tool", status="queued", payload={"cep": "89254300"}, created_at=datetime.now(UTC).replace(tzinfo=None)),
            OperationJob(id=2, job_type="process_search_job", requested_by="tool", status="queued", payload={"cep": "01001000"}, created_at=datetime.now(UTC).replace(tzinfo=None)),
        ]

        from src.services import ops as ops_module

        original_builder = ops_module.build_latest_price_map
        try:
            ops_module.build_latest_price_map = lambda db, cep=None: {
                1: PriceSnapshot(
                    source_product_id=1,
                    price=10.0,
                    availability="available",
                    captured_at=datetime.now(UTC).replace(tzinfo=None),
                    scrape_run_id=1,
                    cep=cep,
                )
            } if cep == "89254300" else {}
            payload = _ops_metrics_payload(session, "89254300")
        finally:
            ops_module.build_latest_price_map = original_builder

        self.assertEqual(payload["requested_cep"], "89254300")
        self.assertEqual(payload["catalog"]["source_products"], 1)
        self.assertEqual(payload["catalog_requests"]["total"], 1)
        self.assertEqual(payload["tracked_items"]["total"], 1)
        self.assertEqual(payload["queue"]["total_jobs"], 1)
        self.assertEqual(payload["operation_jobs"]["total_jobs"], 1)

    def test_live_health_payload_reports_alive_status(self):
        payload = _live_health_payload()
        self.assertEqual(payload["status"], "alive")
        self.assertIn("timestamp", payload)

    def test_readiness_health_payload_reports_ready_with_valid_runtime(self):
        session = _FakeSession([])
        payload = _readiness_health_payload(session)

        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["checks"]["database"]["status"], "ok")
        self.assertEqual(payload["checks"]["config"]["status"], "ok")

    def test_scraper_health_payload_marks_failed_latest_run_as_degraded(self):
        session = _FakeSession([])
        pharmacy = Pharmacy(id=1, name="Panvel", slug="panvel")
        now = datetime.now(UTC).replace(tzinfo=None)
        session.scrape_runs = [
            ScrapeRun(id=2, pharmacy=pharmacy, status="failed", started_at=now),
            ScrapeRun(id=1, pharmacy=pharmacy, status="completed", started_at=now - timedelta(hours=2), finished_at=now - timedelta(hours=2)),
        ]

        from src.services import external_health as external_health_module

        original_metrics = external_health_module.pharmacy_metrics
        try:
            external_health_module.pharmacy_metrics = lambda db, latest_prices=None, cep=None: {
                "Panvel": {
                    "source_products": 10,
                    "matched_products": 8,
                    "match_rate": 0.8,
                    "availability_counts": {"available": 7, "unknown": 2, "out_of_stock": 1},
                    "latest_snapshot_age_minutes": 30,
                }
            }
            payload = _scraper_health_payload(
                session,
                registry_entries=[
                    {
                        "pharmacy_slug": "panvel",
                        "pharmacy": "Panvel",
                        "runtime": "browser",
                        "runtime_enabled": True,
                        "base_domain": "https://www.panvel.com",
                    }
                ],
            )
        finally:
            external_health_module.pharmacy_metrics = original_metrics

        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["pharmacies"][0]["status"], "degraded")
        self.assertEqual(payload["pharmacies"][0]["failure_streak"], 1)

    def test_page_health_payload_marks_failed_probe_as_degraded(self):
        probe_specs = [
            {
                "pharmacy_slug": "panvel",
                "pharmacy": "Panvel",
                "runtime": "browser",
                "runtime_enabled": True,
                "probes": [
                    {
                        "probe_name": "homepage",
                        "url": "https://www.panvel.com",
                        "response_type": "html",
                        "expected_content_type": "text/html",
                    },
                    {
                        "probe_name": "search_page",
                        "url": "https://www.panvel.com/panvel/buscarProduto.do?termoPesquisa=dipirona",
                        "response_type": "html",
                        "expected_content_type": "text/html",
                        "contains_any": ["dipirona"],
                    },
                ],
            }
        ]

        def _fake_fetcher(spec, timeout_seconds):
            if spec["probe_name"] == "homepage":
                return {
                    "status_code": 200,
                    "content_type": "text/html; charset=utf-8",
                    "body_text": "<html><body>Panvel</body></html>",
                    "latency_ms": 12.0,
                }
            return {
                "status_code": 200,
                "content_type": "application/json",
                "body_text": "{}",
                "latency_ms": 15.0,
            }

        payload = _page_health_payload(probe_specs=probe_specs, fetcher=_fake_fetcher)

        self.assertEqual(payload["status"], "degraded")
        self.assertEqual(payload["summary"]["probes_failed"], 1)
        self.assertEqual(payload["pharmacies"][0]["status"], "degraded")

    def test_parse_collection_slots_uses_configured_labels(self):
        slots = parse_collection_slots("08:00,15:00")
        self.assertEqual([slot["name"] for slot in slots], ["morning", "afternoon"])
        self.assertEqual([slot["label"] for slot in slots], ["08:00", "15:00"])

    def test_collection_schedule_status_marks_due_window(self):
        now = datetime(2026, 3, 20, 8, 30, tzinfo=UTC)
        payload = collection_schedule_status(now)
        self.assertTrue(payload["due_now"])
        self.assertEqual(payload["current_slot"]["name"], "morning")

    def test_collection_schedule_status_returns_next_slot_outside_window(self):
        now = datetime(2026, 3, 20, 10, 31, tzinfo=UTC)
        payload = collection_schedule_status(now)
        self.assertFalse(payload["due_now"])
        self.assertEqual(payload["next_slot"]["name"], "afternoon")


if __name__ == "__main__":
    unittest.main()
