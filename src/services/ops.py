from datetime import UTC, datetime

from sqlalchemy import case, func, text

from src.core.config import settings
from src.models.base import CatalogRequest, CanonicalProduct, Pharmacy, PriceSnapshot, ProductMatch, ScrapeRun, SourceProduct, TrackedItemByCep
from src.services.catalog_queries import build_latest_price_map, data_age_minutes, freshness_status, normalize_cep, validate_cep_context
from src.services.demand_tracking import queue_metrics
from src.services.operation_jobs import operation_job_metrics
from src.services.scraper_registry import SCRAPER_REGISTRY

SERVICE_NAME = "super-melhor-preco-farmacia"


def live_health_payload():
    return {
        "status": "alive",
        "service": SERVICE_NAME,
        "timestamp": datetime.now(UTC).replace(tzinfo=None),
    }


def _config_readiness_payload():
    issues = []
    normalized_cep = normalize_cep(settings.CEP)
    if len(normalized_cep) != 8:
        issues.append("CEP configurado invalido; esperado CEP com 8 digitos.")
    if settings.PRICE_RETENTION_DAYS <= 0:
        issues.append("PRICE_RETENTION_DAYS deve ser maior que zero.")
    return {
        "status": "ok" if not issues else "error",
        "active_cep": settings.CEP,
        "issues": issues,
    }


def _scraper_registry_readiness_payload():
    http_scrapers = sum(1 for _, runtime_type, _ in SCRAPER_REGISTRY if runtime_type == "http")
    browser_scrapers = sum(1 for _, runtime_type, _ in SCRAPER_REGISTRY if runtime_type == "browser")
    issues = []
    if not SCRAPER_REGISTRY:
        issues.append("Nenhum scraper registrado no runtime atual.")
    return {
        "status": "ok" if not issues else "error",
        "total": len(SCRAPER_REGISTRY),
        "http_scrapers": http_scrapers,
        "browser_scrapers": browser_scrapers,
        "issues": issues,
    }


def readiness_health_payload(db):
    database_check = {"status": "ok", "message": "database ok"}
    try:
        db.execute(text("SELECT 1"))
    except Exception as exc:
        database_check = {"status": "error", "message": str(exc)[:200]}

    checks = {
        "database": database_check,
        "config": _config_readiness_payload(),
        "scrapers": _scraper_registry_readiness_payload(),
    }
    ready = all(check["status"] == "ok" for check in checks.values())
    return {
        "status": "ready" if ready else "not_ready",
        "service": SERVICE_NAME,
        "timestamp": datetime.now(UTC).replace(tzinfo=None),
        "checks": checks,
    }


def scrape_run_payload(run: ScrapeRun):
    duration_seconds = None
    if run.finished_at:
        duration_seconds = max(int((run.finished_at - run.started_at).total_seconds()), 0)
    return {
        "scrape_run_id": run.id,
        "pharmacy": run.pharmacy.name if run.pharmacy else None,
        "cep": run.cep,
        "trigger_type": run.trigger_type,
        "status": run.status,
        "search_terms": run.search_terms or [],
        "products_seen": run.products_seen,
        "products_saved": run.products_saved,
        "error_count": run.error_count,
        "error_message": run.error_message,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": duration_seconds,
    }


def _empty_pharmacy_bucket():
    return {
        "source_products": 0,
        "matched_products": 0,
        "auto_approved_matches": 0,
        "needs_review_matches": 0,
        "availability_counts": {"available": 0, "unknown": 0, "out_of_stock": 0},
        "latest_snapshot_age_minutes": None,
    }


def _row_value(row, attribute: str, index: int):
    return getattr(row, attribute, row[index])


def _pharmacy_metrics_fallback(db, latest_prices=None, cep: str | None = None):
    latest_prices = latest_prices if latest_prices is not None else build_latest_price_map(db, cep)
    source_product_ids = set(latest_prices.keys()) if latest_prices else set()
    source_products = db.query(SourceProduct).all()
    if cep:
        source_products = [product for product in source_products if product.id in source_product_ids]
    metrics = {}

    for product in source_products:
        pharmacy_name = product.pharmacy.name
        bucket = metrics.setdefault(
            pharmacy_name,
            _empty_pharmacy_bucket(),
        )
        bucket["source_products"] += 1
        if product.match:
            bucket["matched_products"] += 1
            if product.match.review_status == "auto_approved":
                bucket["auto_approved_matches"] += 1
            if product.match.review_status == "needs_review":
                bucket["needs_review_matches"] += 1

        latest_snapshot = latest_prices.get(product.id)
        if latest_snapshot:
            availability = latest_snapshot.availability or "unknown"
            bucket["availability_counts"][availability] = bucket["availability_counts"].get(availability, 0) + 1
            age = data_age_minutes(latest_snapshot.captured_at)
            current_oldest = bucket["latest_snapshot_age_minutes"]
            bucket["latest_snapshot_age_minutes"] = age if current_oldest is None else max(current_oldest, age)

    for bucket in metrics.values():
        bucket["match_rate"] = round(bucket["matched_products"] / bucket["source_products"], 4) if bucket["source_products"] else 0.0
        bucket["auto_approved_rate"] = (
            round(bucket["auto_approved_matches"] / bucket["matched_products"], 4) if bucket["matched_products"] else 0.0
        )

    return metrics


def pharmacy_metrics(db, latest_prices=None, cep: str | None = None):
    latest_prices = latest_prices if latest_prices is not None else build_latest_price_map(db, cep)
    source_product_ids = list(latest_prices.keys())
    if cep and not source_product_ids:
        return {}
    try:
        metric_query = (
            db.query(
                Pharmacy.name.label("pharmacy"),
                func.count(SourceProduct.id).label("source_products"),
                func.count(ProductMatch.id).label("matched_products"),
                func.coalesce(func.sum(case((ProductMatch.review_status == "auto_approved", 1), else_=0)), 0).label("auto_approved_matches"),
                func.coalesce(func.sum(case((ProductMatch.review_status == "needs_review", 1), else_=0)), 0).label("needs_review_matches"),
            )
            .join(SourceProduct, SourceProduct.pharmacy_id == Pharmacy.id)
            .outerjoin(ProductMatch, ProductMatch.source_product_id == SourceProduct.id)
        )
        if cep:
            metric_query = metric_query.filter(SourceProduct.id.in_(source_product_ids))
        metric_rows = metric_query.group_by(Pharmacy.name).all()

        source_product_rows = []
        if source_product_ids:
            source_product_rows = (
                db.query(SourceProduct.id, Pharmacy.name)
                .join(Pharmacy, SourceProduct.pharmacy_id == Pharmacy.id)
                .filter(SourceProduct.id.in_(source_product_ids))
                .all()
            )
    except (AttributeError, TypeError, AssertionError):
        return _pharmacy_metrics_fallback(db, latest_prices=latest_prices, cep=cep)

    metrics = {}
    for row in metric_rows:
        pharmacy_name = _row_value(row, "pharmacy", 0)
        metrics[pharmacy_name] = {
            **_empty_pharmacy_bucket(),
            "source_products": int(_row_value(row, "source_products", 1) or 0),
            "matched_products": int(_row_value(row, "matched_products", 2) or 0),
            "auto_approved_matches": int(_row_value(row, "auto_approved_matches", 3) or 0),
            "needs_review_matches": int(_row_value(row, "needs_review_matches", 4) or 0),
        }

    pharmacy_by_source_product_id = {
        _row_value(row, "id", 0): _row_value(row, "name", 1)
        for row in source_product_rows
    }
    for source_product_id, latest_snapshot in latest_prices.items():
        pharmacy_name = pharmacy_by_source_product_id.get(source_product_id)
        if not pharmacy_name:
            continue
        bucket = metrics.setdefault(pharmacy_name, _empty_pharmacy_bucket())
        availability = latest_snapshot.availability or "unknown"
        bucket["availability_counts"][availability] = bucket["availability_counts"].get(availability, 0) + 1
        age = data_age_minutes(latest_snapshot.captured_at)
        current_oldest = bucket["latest_snapshot_age_minutes"]
        bucket["latest_snapshot_age_minutes"] = age if current_oldest is None else max(current_oldest, age)

    for bucket in metrics.values():
        bucket["match_rate"] = round(bucket["matched_products"] / bucket["source_products"], 4) if bucket["source_products"] else 0.0
        bucket["auto_approved_rate"] = (
            round(bucket["auto_approved_matches"] / bucket["matched_products"], 4) if bucket["matched_products"] else 0.0
        )

    return metrics


def ops_health_payload(db, cep: str | None = None):
    normalized_cep = validate_cep_context(cep) if cep else None
    queue = queue_metrics(db, normalized_cep)
    operation_queue = operation_job_metrics(db, normalized_cep)
    runs_query = db.query(ScrapeRun)
    if normalized_cep:
        runs_query = runs_query.filter(ScrapeRun.cep == normalized_cep)
    last_runs = runs_query.order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc()).limit(20).all()
    last_run_by_pharmacy = {}
    for run in last_runs:
        pharmacy_name = run.pharmacy.name if run.pharmacy else f"pharmacy:{run.pharmacy_id}"
        last_run_by_pharmacy.setdefault(pharmacy_name, run)

    stale_pharmacies = []
    failed_pharmacies = []
    for pharmacy_name, run in last_run_by_pharmacy.items():
        if run.status == "failed":
            failed_pharmacies.append(pharmacy_name)
        elif freshness_status(run.started_at) != "fresh":
            stale_pharmacies.append(pharmacy_name)

    overall_status = "healthy"
    if failed_pharmacies:
        overall_status = "degraded"
    elif queue["queued_jobs"] > 20 or stale_pharmacies:
        overall_status = "attention"

    return {
        "status": overall_status,
        "active_cep": settings.CEP,
        "requested_cep": normalized_cep,
        "queue": queue,
        "operation_jobs": operation_queue,
        "stale_pharmacies": sorted(stale_pharmacies),
        "failed_pharmacies": sorted(failed_pharmacies),
        "last_scrape_runs": [scrape_run_payload(run) for run in last_run_by_pharmacy.values()],
    }


def ops_metrics_payload(db, cep: str | None = None):
    normalized_cep = validate_cep_context(cep) if cep else None
    latest_prices = build_latest_price_map(db, normalized_cep)
    latest_snapshots = list(latest_prices.values())
    source_product_ids = list(latest_prices.keys())
    source_product_id_set = set(source_product_ids)
    availability_counts = {"available": 0, "unknown": 0, "out_of_stock": 0}
    for snapshot in latest_snapshots:
        availability = snapshot.availability or "unknown"
        availability_counts[availability] = availability_counts.get(availability, 0) + 1

    try:
        match_type_query = db.query(ProductMatch.match_type, func.count(ProductMatch.id))
        review_status_query = db.query(ProductMatch.review_status, func.count(ProductMatch.id))
        if normalized_cep:
            if not source_product_ids:
                match_type_rows = []
                review_status_rows = []
                total_matches = 0
            else:
                match_type_rows = match_type_query.filter(ProductMatch.source_product_id.in_(source_product_ids)).group_by(ProductMatch.match_type).all()
                review_status_rows = review_status_query.filter(ProductMatch.source_product_id.in_(source_product_ids)).group_by(ProductMatch.review_status).all()
                total_matches = sum(int(_row_value(row, "count", 1) or 0) for row in match_type_rows)
        else:
            match_type_rows = match_type_query.group_by(ProductMatch.match_type).all()
            review_status_rows = review_status_query.group_by(ProductMatch.review_status).all()
            total_matches = sum(int(_row_value(row, "count", 1) or 0) for row in match_type_rows)
        match_type_counts = {_row_value(row, "match_type", 0): int(_row_value(row, "count", 1) or 0) for row in match_type_rows}
        review_status_counts = {_row_value(row, "review_status", 0): int(_row_value(row, "count", 1) or 0) for row in review_status_rows}
    except (AttributeError, TypeError, AssertionError):
        matches = db.query(ProductMatch).all()
        if normalized_cep:
            matches = [match for match in matches if match.source_product_id in source_product_id_set]
        match_type_counts = {}
        review_status_counts = {}
        for match in matches:
            match_type_counts[match.match_type] = match_type_counts.get(match.match_type, 0) + 1
            review_status_counts[match.review_status] = review_status_counts.get(match.review_status, 0) + 1
        total_matches = len(matches)

    if normalized_cep:
        catalog_requests_query = db.query(CatalogRequest).filter(CatalogRequest.cep == normalized_cep)
        tracked_items_query = db.query(TrackedItemByCep).filter(TrackedItemByCep.cep == normalized_cep)
        queue = queue_metrics(db, normalized_cep)
        operation_jobs = operation_job_metrics(db, normalized_cep)
        source_products_count = len(source_product_ids)
        canonical_products_count = len({match.canonical_product_id for match in matches}) if "matches" in locals() else 0
        if "matches" not in locals() and source_product_ids:
            try:
                canonical_products_count = (
                    db.query(func.count(func.distinct(ProductMatch.canonical_product_id)))
                    .filter(ProductMatch.source_product_id.in_(source_product_ids))
                    .scalar()
                    or 0
                )
            except (AttributeError, TypeError, AssertionError):
                canonical_products_count = 0
    else:
        catalog_requests_query = db.query(CatalogRequest)
        tracked_items_query = db.query(TrackedItemByCep)
        queue = queue_metrics(db)
        operation_jobs = operation_job_metrics(db)
        source_products_count = db.query(SourceProduct).count()
        canonical_products_count = db.query(CanonicalProduct).count()

    return {
        "active_cep": settings.CEP,
        "requested_cep": normalized_cep,
        "catalog": {
            "canonical_products": canonical_products_count,
            "source_products": source_products_count,
            "latest_snapshots": len(latest_snapshots),
        },
        "matching": {
            "total_matches": total_matches,
            "match_type_counts": match_type_counts,
            "review_status_counts": review_status_counts,
        },
        "availability": availability_counts,
        "queue": queue,
        "operation_jobs": operation_jobs,
        "catalog_requests": {
            "pending": catalog_requests_query.filter(CatalogRequest.status == "pending").count(),
            "total": catalog_requests_query.count(),
        },
        "tracked_items": {
            "total": tracked_items_query.count(),
            "active": tracked_items_query.filter(TrackedItemByCep.status == "active").count(),
            "cooldown": tracked_items_query.filter(TrackedItemByCep.status == "cooldown").count(),
            "inactive": tracked_items_query.filter(TrackedItemByCep.status == "inactive").count(),
        },
        "pharmacies": pharmacy_metrics(db, latest_prices=latest_prices, cep=normalized_cep),
    }
