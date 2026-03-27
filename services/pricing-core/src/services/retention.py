from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, delete, func, or_

from src.core.config import settings
from src.models.base import CatalogRequest, OperationJob, PriceSnapshot, ScrapeRun, SearchJob, SessionLocal, TrackedItemByCep

SEARCH_JOB_TERMINAL_STATUSES = ("completed", "partial_success", "failed", "skipped")
OPERATION_JOB_TERMINAL_STATUSES = ("completed", "failed")


def _retention_cutoff(*, retention_days: int | None = None, now: datetime | None = None):
    retention_days = retention_days or settings.PRICE_RETENTION_DAYS
    reference_now = now or datetime.now(UTC).replace(tzinfo=None)
    return reference_now - timedelta(days=retention_days), retention_days


def _timestamp_or_none(value):
    return value if isinstance(value, datetime) else None


def _session_supports_bulk_delete(session) -> bool:
    return not hasattr(session, "price_snapshots")


def _delete_matching_records(session, model, items, predicate):
    deleted_count = 0
    for item in list(items):
        if predicate(item):
            session.delete(item)
            deleted_count += 1
    if deleted_count:
        session.commit()
    return deleted_count


def purge_expired_operational_data(*, retention_days: int | None = None, session_factory=None, now: datetime | None = None):
    session_factory = session_factory or SessionLocal
    with session_factory() as session:
        return purge_expired_operational_data_in_session(session, retention_days=retention_days, now=now)


def purge_expired_operational_data_in_session(session, *, retention_days: int | None = None, now: datetime | None = None):
    cutoff, retention_days = _retention_cutoff(retention_days=retention_days, now=now)

    if _session_supports_bulk_delete(session):
        active_catalog_request_ids = session.query(SearchJob.catalog_request_id).filter(SearchJob.catalog_request_id.isnot(None))

        deleted_snapshots = session.execute(delete(PriceSnapshot).where(PriceSnapshot.captured_at < cutoff)).rowcount or 0
        deleted_scrape_runs = (
            session.execute(
                delete(ScrapeRun).where(
                    or_(
                        ScrapeRun.finished_at < cutoff,
                        and_(ScrapeRun.finished_at.is_(None), ScrapeRun.started_at < cutoff),
                    )
                )
            ).rowcount
            or 0
        )
        deleted_search_jobs = (
            session.execute(
                delete(SearchJob).where(
                    SearchJob.status.in_(SEARCH_JOB_TERMINAL_STATUSES),
                    func.coalesce(SearchJob.finished_at, SearchJob.updated_at, SearchJob.created_at) < cutoff,
                )
            ).rowcount
            or 0
        )
        deleted_operation_jobs = (
            session.execute(
                delete(OperationJob).where(
                    OperationJob.status.in_(OPERATION_JOB_TERMINAL_STATUSES),
                    func.coalesce(OperationJob.finished_at, OperationJob.updated_at, OperationJob.created_at) < cutoff,
                )
            ).rowcount
            or 0
        )
        deleted_catalog_requests = (
            session.execute(
                delete(CatalogRequest).where(
                    CatalogRequest.last_requested_at < cutoff,
                    ~CatalogRequest.id.in_(active_catalog_request_ids),
                )
            ).rowcount
            or 0
        )
        deleted_tracked_items = (
            session.execute(delete(TrackedItemByCep).where(TrackedItemByCep.last_requested_at < cutoff)).rowcount or 0
        )
        session.commit()
    else:
        deleted_snapshots = _delete_matching_records(
            session,
            PriceSnapshot,
            getattr(session, "price_snapshots", []),
            lambda item: _timestamp_or_none(getattr(item, "captured_at", None)) and item.captured_at < cutoff,
        )
        deleted_scrape_runs = _delete_matching_records(
            session,
            ScrapeRun,
            getattr(session, "scrape_runs", []),
            lambda item: (
                (_timestamp_or_none(getattr(item, "finished_at", None)) and item.finished_at < cutoff)
                or (
                    not _timestamp_or_none(getattr(item, "finished_at", None))
                    and _timestamp_or_none(getattr(item, "started_at", None))
                    and item.started_at < cutoff
                )
            ),
        )
        deleted_search_jobs = _delete_matching_records(
            session,
            SearchJob,
            getattr(session, "search_jobs", []),
            lambda item: getattr(item, "status", None) in SEARCH_JOB_TERMINAL_STATUSES
            and (
                _timestamp_or_none(getattr(item, "finished_at", None))
                or _timestamp_or_none(getattr(item, "updated_at", None))
                or _timestamp_or_none(getattr(item, "created_at", None))
            )
            and (
                (_timestamp_or_none(getattr(item, "finished_at", None)) or _timestamp_or_none(getattr(item, "updated_at", None)) or _timestamp_or_none(getattr(item, "created_at", None)))
                < cutoff
            ),
        )
        deleted_operation_jobs = _delete_matching_records(
            session,
            OperationJob,
            getattr(session, "operation_jobs", []),
            lambda item: getattr(item, "status", None) in OPERATION_JOB_TERMINAL_STATUSES
            and (
                _timestamp_or_none(getattr(item, "finished_at", None))
                or _timestamp_or_none(getattr(item, "updated_at", None))
                or _timestamp_or_none(getattr(item, "created_at", None))
            )
            and (
                (_timestamp_or_none(getattr(item, "finished_at", None)) or _timestamp_or_none(getattr(item, "updated_at", None)) or _timestamp_or_none(getattr(item, "created_at", None)))
                < cutoff
            ),
        )
        active_catalog_request_ids = {
            job.catalog_request_id
            for job in getattr(session, "search_jobs", [])
            if getattr(job, "catalog_request_id", None)
        }
        deleted_catalog_requests = _delete_matching_records(
            session,
            CatalogRequest,
            getattr(session, "catalog_requests", []),
            lambda item: _timestamp_or_none(getattr(item, "last_requested_at", None))
            and item.last_requested_at < cutoff
            and item.id not in active_catalog_request_ids,
        )
        deleted_tracked_items = _delete_matching_records(
            session,
            TrackedItemByCep,
            getattr(session, "tracked_items", []),
            lambda item: _timestamp_or_none(getattr(item, "last_requested_at", None)) and item.last_requested_at < cutoff,
        )

    return {
        "retention_days": retention_days,
        "cutoff": cutoff.isoformat(),
        "deleted_snapshots": deleted_snapshots,
        "deleted_scrape_runs": deleted_scrape_runs,
        "deleted_search_jobs": deleted_search_jobs,
        "deleted_operation_jobs": deleted_operation_jobs,
        "deleted_catalog_requests": deleted_catalog_requests,
        "deleted_tracked_items": deleted_tracked_items,
    }
