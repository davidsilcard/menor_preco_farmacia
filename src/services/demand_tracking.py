from datetime import UTC, datetime

from sqlalchemy.orm import Session

from src.models.base import CatalogRequest, CanonicalProduct, SearchJob, TrackedItemByCep
from src.services.catalog_queries import data_age_minutes, normalize_query

SEARCH_JOB_ETA_SECONDS = 15 * 60
TRACKED_ITEM_ACTIVE_DAYS = 30
TRACKED_ITEM_INACTIVE_DAYS = 90


def register_catalog_request(db: Session, query: str, cep: str, tool_name: str):
    normalized_query = normalize_query(query)
    if not normalized_query:
        return None

    existing = (
        db.query(CatalogRequest)
        .filter(CatalogRequest.normalized_query == normalized_query, CatalogRequest.cep == cep)
        .first()
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    if existing:
        existing.request_count += 1
        existing.last_requested_at = now
        existing.last_requested_by_tool = tool_name
        db.commit()
        db.refresh(existing)
        return existing

    request = CatalogRequest(
        query=query,
        normalized_query=normalized_query,
        cep=cep,
        status="pending",
        request_count=1,
        first_requested_at=now,
        last_requested_at=now,
        last_requested_by_tool=tool_name,
    )
    db.add(request)
    db.commit()
    db.refresh(request)
    return request


def catalog_request_payload(request: CatalogRequest | None):
    if not request:
        return None
    return {
        "catalog_request_id": request.id,
        "query": request.query,
        "normalized_query": request.normalized_query,
        "cep": request.cep,
        "status": request.status,
        "request_count": request.request_count,
        "first_requested_at": request.first_requested_at,
        "last_requested_at": request.last_requested_at,
        "last_requested_by_tool": request.last_requested_by_tool,
    }


def tracked_item_status(last_requested_at):
    age_minutes = data_age_minutes(last_requested_at)
    if age_minutes is None:
        return "active"
    age_days = age_minutes / (60 * 24)
    if age_days > TRACKED_ITEM_INACTIVE_DAYS:
        return "inactive"
    if age_days > TRACKED_ITEM_ACTIVE_DAYS:
        return "cooldown"
    return "active"


def tracked_item_priority(request_count_total: int, last_requested_at, canonical_product_id: int | None):
    age_minutes = data_age_minutes(last_requested_at) or 0
    age_days = age_minutes / (60 * 24)
    status = tracked_item_status(last_requested_at)
    if status == "inactive":
        return 0.0

    base = 100.0 if status == "active" else 40.0
    demand_bonus = min(request_count_total, 25) * 2.0
    recency_penalty = min(age_days, 30) * 1.5
    canonical_bonus = 10.0 if canonical_product_id else 0.0
    return round(max(base + demand_bonus + canonical_bonus - recency_penalty, 0.0), 2)


def tracked_item_payload(item: TrackedItemByCep | None):
    if not item:
        return None
    return {
        "tracked_item_id": item.id,
        "cep": item.cep,
        "query": item.query,
        "normalized_query": item.normalized_query,
        "canonical_product_id": item.canonical_product_id,
        "status": item.status,
        "request_count_total": item.request_count_total,
        "scrape_priority": item.scrape_priority,
        "first_requested_at": item.first_requested_at,
        "last_requested_at": item.last_requested_at,
        "last_scraped_at": item.last_scraped_at,
        "last_requested_by_tool": item.last_requested_by_tool,
        "source_kind": item.source_kind,
        "last_match_confidence": item.last_match_confidence,
    }


def _merge_tracked_items(db: Session, primary: TrackedItemByCep, secondary: TrackedItemByCep):
    if primary.id == secondary.id:
        return primary

    primary.request_count_total += secondary.request_count_total
    primary.first_requested_at = min(primary.first_requested_at, secondary.first_requested_at)
    primary.last_requested_at = max(primary.last_requested_at, secondary.last_requested_at)
    if secondary.last_scraped_at and (not primary.last_scraped_at or secondary.last_scraped_at > primary.last_scraped_at):
        primary.last_scraped_at = secondary.last_scraped_at
    if not primary.canonical_product_id and secondary.canonical_product_id:
        primary.canonical_product_id = secondary.canonical_product_id
    if secondary.last_match_confidence is not None and (
        primary.last_match_confidence is None or secondary.last_match_confidence > primary.last_match_confidence
    ):
        primary.last_match_confidence = secondary.last_match_confidence
    if secondary.source_kind and not primary.source_kind:
        primary.source_kind = secondary.source_kind
    if secondary.last_requested_by_tool:
        primary.last_requested_by_tool = secondary.last_requested_by_tool
    if len(secondary.query or "") > len(primary.query or ""):
        primary.query = secondary.query
        primary.normalized_query = secondary.normalized_query

    db.delete(secondary)
    db.flush()
    return primary


def register_tracked_item(
    db: Session,
    query: str,
    cep: str,
    tool_name: str,
    *,
    canonical_product: CanonicalProduct | None = None,
    source_kind: str | None = None,
    match_confidence: float | None = None,
):
    normalized_query = normalize_query(query)
    if not normalized_query:
        return None

    tracked_by_query = (
        db.query(TrackedItemByCep)
        .filter(TrackedItemByCep.cep == cep, TrackedItemByCep.normalized_query == normalized_query)
        .first()
    )
    tracked_by_canonical = None
    if canonical_product:
        tracked_by_canonical = (
            db.query(TrackedItemByCep)
            .filter(TrackedItemByCep.cep == cep, TrackedItemByCep.canonical_product_id == canonical_product.id)
            .first()
        )

    tracked_item = tracked_by_query or tracked_by_canonical
    if tracked_by_query and tracked_by_canonical and tracked_by_query.id != tracked_by_canonical.id:
        tracked_item = _merge_tracked_items(db, tracked_by_canonical, tracked_by_query)

    now = datetime.now(UTC).replace(tzinfo=None)
    if tracked_item:
        tracked_item.request_count_total += 1
        tracked_item.last_requested_at = now
        tracked_item.last_requested_by_tool = tool_name
        if canonical_product:
            tracked_item.canonical_product_id = canonical_product.id
        if source_kind:
            tracked_item.source_kind = source_kind
        if match_confidence is not None:
            tracked_item.last_match_confidence = match_confidence
    else:
        tracked_item = TrackedItemByCep(
            cep=cep,
            query=query,
            normalized_query=normalized_query,
            canonical_product_id=canonical_product.id if canonical_product else None,
            status="active",
            request_count_total=1,
            first_requested_at=now,
            last_requested_at=now,
            last_requested_by_tool=tool_name,
            source_kind=source_kind,
            last_match_confidence=match_confidence,
        )
        db.add(tracked_item)
        db.flush()

    tracked_item.status = tracked_item_status(tracked_item.last_requested_at)
    tracked_item.scrape_priority = tracked_item_priority(
        tracked_item.request_count_total,
        tracked_item.last_requested_at,
        tracked_item.canonical_product_id,
    )
    db.commit()
    db.refresh(tracked_item)
    return tracked_item


def queued_job_position(db: Session, current_job_id: int | None = None):
    queued_jobs = (
        db.query(SearchJob)
        .filter(SearchJob.status.in_(["queued", "processing"]))
        .order_by(SearchJob.created_at.asc(), SearchJob.id.asc())
        .all()
    )
    if current_job_id is None:
        return len(queued_jobs) + 1
    for index, job in enumerate(queued_jobs, start=1):
        if job.id == current_job_id:
            return index
    return len(queued_jobs) + 1


def search_job_payload(job: SearchJob | None, db: Session | None = None):
    if not job:
        return None
    position = job.position_hint
    if db and job.status in {"queued", "processing"}:
        position = queued_job_position(db, job.id)
    return {
        "job_id": job.id,
        "query": job.query,
        "normalized_query": job.normalized_query,
        "cep": job.cep,
        "status": job.status,
        "warnings": ((job.result_payload or {}).get("warnings") or []),
        "requested_by_tool": job.requested_by_tool,
        "request_count": job.request_count,
        "position": position,
        "eta_seconds": job.eta_seconds,
        "catalog_request_id": job.catalog_request_id,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "error_message": job.error_message,
        "result_payload": job.result_payload,
    }


def register_search_job(
    db: Session,
    query: str,
    cep: str,
    tool_name: str,
    catalog_request: CatalogRequest | None = None,
):
    normalized_query = normalize_query(query)
    if not normalized_query:
        return None

    existing = (
        db.query(SearchJob)
        .filter(
            SearchJob.normalized_query == normalized_query,
            SearchJob.cep == cep,
            SearchJob.status.in_(["queued", "processing"]),
        )
        .order_by(SearchJob.created_at.asc(), SearchJob.id.asc())
        .first()
    )
    now = datetime.now(UTC).replace(tzinfo=None)
    if existing:
        existing.request_count += 1
        existing.requested_by_tool = tool_name
        existing.updated_at = now
        if catalog_request and not existing.catalog_request_id:
            existing.catalog_request_id = catalog_request.id
        existing.position_hint = queued_job_position(db, existing.id)
        existing.eta_seconds = max(existing.position_hint - 1, 0) * SEARCH_JOB_ETA_SECONDS
        db.commit()
        db.refresh(existing)
        return existing

    job = SearchJob(
        query=query,
        normalized_query=normalized_query,
        cep=cep,
        status="queued",
        requested_by_tool=tool_name,
        request_count=1,
        catalog_request_id=catalog_request.id if catalog_request else None,
        created_at=now,
        updated_at=now,
    )
    db.add(job)
    db.flush()
    job.position_hint = queued_job_position(db, job.id)
    job.eta_seconds = max(job.position_hint - 1, 0) * SEARCH_JOB_ETA_SECONDS
    db.commit()
    db.refresh(job)
    return job


def queue_metrics(db: Session):
    jobs = db.query(SearchJob).all()
    queued = [job for job in jobs if job.status == "queued"]
    processing = [job for job in jobs if job.status == "processing"]
    failed = [job for job in jobs if job.status == "failed"]
    completed = [job for job in jobs if job.status == "completed"]
    return {
        "total_jobs": len(jobs),
        "queued_jobs": len(queued),
        "processing_jobs": len(processing),
        "completed_jobs": len(completed),
        "failed_jobs": len(failed),
        "oldest_queued_job_minutes": (
            data_age_minutes(min(job.created_at for job in queued))
            if queued
            else None
        ),
    }
