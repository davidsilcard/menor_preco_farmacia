from src.core.config import settings
from src.models.base import CatalogRequest, CanonicalProduct, ProductMatch, ScrapeRun, SourceProduct, TrackedItemByCep
from src.services.catalog_queries import build_latest_price_map, data_age_minutes, freshness_status
from src.services.demand_tracking import queue_metrics


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


def pharmacy_metrics(db):
    source_products = db.query(SourceProduct).all()
    latest_prices = build_latest_price_map(db)
    metrics = {}

    for product in source_products:
        pharmacy_name = product.pharmacy.name
        bucket = metrics.setdefault(
            pharmacy_name,
            {
                "source_products": 0,
                "matched_products": 0,
                "auto_approved_matches": 0,
                "needs_review_matches": 0,
                "availability_counts": {"available": 0, "unknown": 0, "out_of_stock": 0},
                "latest_snapshot_age_minutes": None,
            },
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


def ops_health_payload(db):
    queue = queue_metrics(db)
    last_runs = db.query(ScrapeRun).order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc()).limit(20).all()
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
        "queue": queue,
        "stale_pharmacies": sorted(stale_pharmacies),
        "failed_pharmacies": sorted(failed_pharmacies),
        "last_scrape_runs": [scrape_run_payload(run) for run in last_run_by_pharmacy.values()],
    }


def ops_metrics_payload(db):
    latest_prices = build_latest_price_map(db)
    matches = db.query(ProductMatch).all()
    latest_snapshots = list(latest_prices.values())
    availability_counts = {"available": 0, "unknown": 0, "out_of_stock": 0}
    for snapshot in latest_snapshots:
        availability = snapshot.availability or "unknown"
        availability_counts[availability] = availability_counts.get(availability, 0) + 1

    match_type_counts = {}
    review_status_counts = {}
    for match in matches:
        match_type_counts[match.match_type] = match_type_counts.get(match.match_type, 0) + 1
        review_status_counts[match.review_status] = review_status_counts.get(match.review_status, 0) + 1

    return {
        "active_cep": settings.CEP,
        "catalog": {
            "canonical_products": db.query(CanonicalProduct).count(),
            "source_products": db.query(SourceProduct).count(),
            "latest_snapshots": len(latest_snapshots),
        },
        "matching": {
            "total_matches": len(matches),
            "match_type_counts": match_type_counts,
            "review_status_counts": review_status_counts,
        },
        "availability": availability_counts,
        "queue": queue_metrics(db),
        "catalog_requests": {
            "pending": db.query(CatalogRequest).filter(CatalogRequest.status == "pending").count(),
            "total": db.query(CatalogRequest).count(),
        },
        "tracked_items": {
            "total": db.query(TrackedItemByCep).count(),
            "active": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "active").count(),
            "cooldown": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "cooldown").count(),
            "inactive": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "inactive").count(),
        },
        "pharmacies": pharmacy_metrics(db),
    }
