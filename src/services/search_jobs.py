import asyncio
import inspect
import logging
from datetime import UTC, datetime

from src.core.config import settings
from src.core.logging import get_logger, log_event
from src.models.base import CatalogRequest, SearchJob, SessionLocal
from src.services.catalog_queries import (
    best_pricing_offer,
    build_latest_price_map,
    build_cmed_reference_map,
    canonical_offer_payload,
    find_matching_canonicals,
    find_matching_canonicals_from_source_products,
    preferred_search_terms,
)
from src.services.demand_tracking import sync_tracked_item_with_search_results
from src.services.scraper_registry import SCRAPER_REGISTRY
from src.services.tool_use import item_availability_summary

LOGGER = get_logger(__name__)


def _json_safe(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _job_warnings(scraper_results: list[dict], search_results: dict):
    warnings = []
    failed_scrapers = [result["pharmacy_slug"] for result in scraper_results if result.get("status") == "failed"]
    skipped_scrapers = [result["pharmacy_slug"] for result in scraper_results if result.get("status") == "skipped"]
    if failed_scrapers:
        warnings.append(
            {
                "code": "partial_scraper_failure",
                "message": "Uma ou mais farmacias falharam durante a busca sob demanda.",
                "pharmacies": failed_scrapers,
            }
        )
    if skipped_scrapers:
        warnings.append(
            {
                "code": "scraper_runtime_unavailable",
                "message": "Parte das farmacias foi pulada porque depende de runtime de browser nao habilitado para busca sob demanda.",
                "pharmacies": skipped_scrapers,
            }
        )

    if not search_results.get("results"):
        warnings.append(
            {
                "code": "no_results_found",
                "message": "Nenhum produto foi encontrado na busca sob demanda atual.",
            }
        )
    return warnings


def _job_completion_status(scraper_results: list[dict]):
    non_completed_count = sum(1 for result in scraper_results if result.get("status") != "completed")
    failed_count = sum(1 for result in scraper_results if result.get("status") == "failed")
    if non_completed_count == 0:
        return "completed"
    if failed_count == len(scraper_results):
        return "failed"
    return "partial_success"


def _run_scraper_for_terms(scraper, terms: list[str], cep: str):
    original_terms = list(getattr(scraper, "search_terms", []) or [])
    original_cep = getattr(scraper, "cep", None)
    scraper.search_terms = list(terms)
    scraper.set_cep(cep)
    try:
        scrape_method = scraper.scrape
        if inspect.iscoroutinefunction(scrape_method):
            products = asyncio.run(scrape_method())
        else:
            products = scrape_method()
        products = products or []
        if products:
            scraper.save_to_db(products)
        return {
            "products_found": len(products),
            "status": "completed",
        }
    finally:
        scraper.search_terms = original_terms
        if original_cep:
            scraper.cep = original_cep


def _job_search_result_payload(session, query: str, cep: str):
    latest_prices = build_latest_price_map(session, cep)
    cmed_reference_map = build_cmed_reference_map(session)
    matches = find_matching_canonicals(session, query)
    result_origin = "canonical_match"
    if not matches:
        matches = find_matching_canonicals_from_source_products(session, query, latest_prices)
        if matches:
            result_origin = "source_product_fallback"
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": offers,
            "best_offer": best_pricing_offer(offers),
            "availability_summary": item_availability_summary(
                {
                    "match_found": True,
                    "best_offer": best_pricing_offer(offers),
                    "offers": offers,
                }
            ),
        }
        for score, canonical_product in matches
        for offers in [canonical_offer_payload(canonical_product, latest_prices, cmed_reference_map)]
    ]
    return {
        "results_found": len(results),
        "results": results,
        "result_origin": result_origin if results else None,
    }


def _complete_job(session, job: SearchJob, *, status: str, result_payload=None, error_message: str | None = None):
    now = datetime.now(UTC).replace(tzinfo=None)
    job.status = status
    job.finished_at = now
    job.updated_at = now
    job.position_hint = None
    job.eta_seconds = 0
    job.result_payload = _json_safe(result_payload)
    job.error_message = error_message

    if job.catalog_request_id:
        catalog_request = session.get(CatalogRequest, job.catalog_request_id)
        if catalog_request:
            if status in {"completed", "partial_success"}:
                has_results = bool((result_payload or {}).get("search_results", {}).get("results"))
                catalog_request.status = "fulfilled" if has_results else "searched_no_results"
            elif status == "failed":
                catalog_request.status = "failed"

    session.commit()
    session.refresh(job)
    return job


def process_search_job(job_id: int | None = None):
    with SessionLocal() as session:
        query = session.query(SearchJob)
        if job_id is not None:
            query = query.filter(SearchJob.id == job_id)
        else:
            query = query.filter(SearchJob.status == "queued").order_by(SearchJob.created_at.asc(), SearchJob.id.asc())

        job = query.first()
        if not job:
            return None

        if job.status not in {"queued", "processing"}:
            return job

        now = datetime.now(UTC).replace(tzinfo=None)
        job.status = "processing"
        job.started_at = job.started_at or now
        job.updated_at = now
        session.commit()
        session.refresh(job)
        log_event(
            LOGGER,
            logging.INFO,
            "search_job_processing_started",
            search_job_id=job.id,
            query=job.query,
            cep=job.cep,
        )

        scraper_results = []
        search_terms = preferred_search_terms(job.query) or [job.query]
        try:
            for scraper_slug, runtime_type, scraper_cls in SCRAPER_REGISTRY:
                if runtime_type == "browser" and not settings.ON_DEMAND_ENABLE_BROWSER_SCRAPERS:
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            "products_found": 0,
                            "status": "skipped",
                            "error_message": "Browser scrapers desabilitadas para busca sob demanda.",
                        }
                    )
                    continue
                scraper = scraper_cls()
                try:
                    result = _run_scraper_for_terms(scraper, search_terms, job.cep)
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            "search_terms": search_terms,
                            **result,
                        }
                    )
                except Exception as exc:
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            "search_terms": search_terms,
                            "products_found": 0,
                            "status": "failed",
                            "error_message": str(exc)[:500],
                        }
                    )

            search_results = _job_search_result_payload(session, job.query, job.cep)
            sync_tracked_item_with_search_results(
                session,
                normalized_query=job.normalized_query,
                cep=job.cep,
                search_results=search_results,
            )
            warnings = _job_warnings(scraper_results, search_results)
            final_status = _job_completion_status(scraper_results)
            payload = {
                "query": job.query,
                "normalized_query": job.normalized_query,
                "cep": job.cep,
                "processed_at": datetime.now(UTC).replace(tzinfo=None),
                "search_terms": search_terms,
                "scrapers": scraper_results,
                "warnings": warnings,
                "totals": {
                    "pharmacies_attempted": len(scraper_results),
                    "pharmacies_with_products": sum(1 for result in scraper_results if result["products_found"] > 0),
                    "products_found": sum(result["products_found"] for result in scraper_results),
                },
                "search_results": search_results,
            }
            completed_job = _complete_job(session, job, status=final_status, result_payload=payload)
            log_event(
                LOGGER,
                logging.INFO,
                "search_job_processing_completed",
                search_job_id=completed_job.id,
                status=completed_job.status,
            )
            return completed_job
        except Exception as exc:
            session.rollback()
            failed_job = _complete_job(session, job, status="failed", error_message=str(exc)[:500])
            log_event(
                LOGGER,
                logging.ERROR,
                "search_job_processing_failed",
                search_job_id=failed_job.id,
                error_message=failed_job.error_message,
            )
            return failed_job


def process_next_search_job():
    return process_search_job()
