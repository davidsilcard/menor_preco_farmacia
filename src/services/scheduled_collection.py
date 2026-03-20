import asyncio
import inspect
from collections import defaultdict
from datetime import UTC, datetime
import re

from src.core.config import settings
from src.models.base import SessionLocal, TrackedItemByCep
from src.scrapers.base import BaseScraper
from src.services.scraper_registry import SCRAPER_REGISTRY

ACTIVE_DAYS = 30
INACTIVE_DAYS = 90


def _tracked_item_status_for_scheduler(last_requested_at):
    if not last_requested_at:
        return "active"
    now = datetime.now(UTC).replace(tzinfo=None)
    age_days = max((now - last_requested_at).total_seconds(), 0) / (60 * 60 * 24)
    if age_days > INACTIVE_DAYS:
        return "inactive"
    if age_days > ACTIVE_DAYS:
        return "cooldown"
    return "active"


def refresh_tracked_items_for_scheduler(session, cep: str | None = None):
    query = session.query(TrackedItemByCep)
    if cep:
        query = query.filter(TrackedItemByCep.cep == cep)

    items = query.all()
    for item in items:
        item.status = _tracked_item_status_for_scheduler(item.last_requested_at)
    session.commit()
    return items


def _group_tracked_items_for_plan(items: list[TrackedItemByCep], *, include_cooldown: bool, limit_per_cep: int):
    grouped = defaultdict(list)
    allowed_statuses = {"active", "cooldown"} if include_cooldown else {"active"}

    ordered_items = sorted(
        [item for item in items if item.status in allowed_statuses],
        key=lambda item: (-float(item.scrape_priority or 0), item.last_requested_at or datetime.min),
    )
    for item in ordered_items:
        if len(grouped[item.cep]) >= limit_per_cep:
            continue
        grouped[item.cep].append(item)
    return grouped


def _collection_search_term(item: TrackedItemByCep):
    normalized = BaseScraper.normalize_text(item.query or "")
    tokens = [token for token in normalized.split() if len(token) >= 2]
    if not tokens:
        return item.query

    alpha_tokens = [token for token in tokens if re.search(r"[a-z]", token)]
    first_alpha = alpha_tokens[0] if alpha_tokens else tokens[0]
    dosage_token = next((token for token in tokens if re.search(r"\d", token) and re.search(r"(mg|ml|g|ui)$", token)), None)

    # Para VTEX e buscas HTTP, termo curto e estavel funciona melhor que descricao longa.
    if item.canonical_product_id and dosage_token:
        return f"{first_alpha} {dosage_token}"
    return first_alpha


def _plan_payload(grouped_items):
    payload = []
    for cep, items in grouped_items.items():
        payload.append(
            {
                "cep": cep,
                "item_count": len(items),
                "queries": [item.query for item in items],
                "search_terms": [_collection_search_term(item) for item in items],
                "tracked_item_ids": [item.id for item in items],
                "statuses": {item.status: sum(1 for candidate in items if candidate.status == item.status) for item in items},
            }
        )
    payload.sort(key=lambda item: item["cep"])
    return payload


def build_scheduled_collection_plan(cep: str | None = None):
    with SessionLocal() as session:
        refresh_tracked_items_for_scheduler(session, cep)
        query = session.query(TrackedItemByCep)
        if cep:
            query = query.filter(TrackedItemByCep.cep == cep)
        items = query.all()
        grouped = _group_tracked_items_for_plan(
            items,
            include_cooldown=True,
            limit_per_cep=settings.SCHEDULED_COLLECTION_MAX_ITEMS_PER_CEP,
        )
        return _plan_payload(grouped)


def _run_scraper_for_terms(scraper, terms: list[str]):
    original_terms = list(getattr(scraper, "search_terms", []) or [])
    scraper.search_terms = list(terms)
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
            "status": "completed",
            "products_found": len(products),
        }
    finally:
        scraper.search_terms = original_terms


def _mark_items_scraped(session, items: list[TrackedItemByCep]):
    now = datetime.now(UTC).replace(tzinfo=None)
    for item in items:
        item.last_scraped_at = now
    session.commit()


def run_scheduled_collection(cep: str | None = None):
    with SessionLocal() as session:
        refresh_tracked_items_for_scheduler(session, cep)
        query = session.query(TrackedItemByCep)
        if cep:
            query = query.filter(TrackedItemByCep.cep == cep)
        items = query.all()
        grouped = _group_tracked_items_for_plan(
            items,
            include_cooldown=True,
            limit_per_cep=settings.SCHEDULED_COLLECTION_MAX_ITEMS_PER_CEP,
        )

        results = []
        for plan_cep, plan_items in grouped.items():
            if plan_cep != settings.CEP:
                results.append(
                    {
                        "cep": plan_cep,
                        "status": "skipped",
                        "message": f"CEP {plan_cep} ainda nao esta habilitado no runtime atual.",
                        "queries": [item.query for item in plan_items],
                    }
                )
                continue

            queries = [item.query for item in plan_items]
            search_terms = list(dict.fromkeys(_collection_search_term(item) for item in plan_items if _collection_search_term(item)))
            scraper_results = []
            for scraper_slug, runtime_type, scraper_cls in SCRAPER_REGISTRY:
                if runtime_type == "browser" and not settings.SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS:
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            "status": "skipped",
                            "products_found": 0,
                            "error_message": "Browser scrapers desabilitadas para coleta agendada.",
                        }
                    )
                    continue

                scraper = scraper_cls()
                try:
                    result = _run_scraper_for_terms(scraper, search_terms)
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            **result,
                        }
                    )
                except Exception as exc:
                    scraper_results.append(
                        {
                            "pharmacy_slug": scraper_slug,
                            "runtime": runtime_type,
                            "status": "failed",
                            "products_found": 0,
                            "error_message": str(exc)[:500],
                        }
                    )

            _mark_items_scraped(session, plan_items)
            results.append(
                    {
                        "cep": plan_cep,
                        "status": "completed",
                        "queries": queries,
                        "search_terms": search_terms,
                        "tracked_item_ids": [item.id for item in plan_items],
                        "scrapers": scraper_results,
                    }
            )

        return {
            "executed_at": datetime.now(UTC).replace(tzinfo=None).isoformat(),
            "plan": _plan_payload(grouped),
            "results": results,
        }
