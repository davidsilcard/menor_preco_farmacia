import json
import logging
from datetime import UTC, datetime

from sqlalchemy import func

from src.core.logging import get_logger, log_event
from src.models.base import OperationJob, SearchJob, SessionLocal

LOGGER = get_logger(__name__)

JOB_TYPE_SCHEDULED_COLLECTION = "scheduled_collection"
JOB_TYPE_OPERATIONAL_CYCLE = "operational_cycle"
JOB_TYPE_PROCESS_NEXT_SEARCH_JOB = "process_next_search_job"
JOB_TYPE_PROCESS_SEARCH_JOB = "process_search_job"


def _json_safe(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value


def _payload_fingerprint(payload: dict | None):
    return json.dumps(payload or {}, sort_keys=True, ensure_ascii=False, default=str)


def operation_job_payload(job: OperationJob | None):
    if not job:
        return None
    return {
        "operation_job_id": job.id,
        "job_type": job.job_type,
        "requested_by": job.requested_by,
        "status": job.status,
        "request_count": job.request_count,
        "payload": job.payload,
        "result_payload": job.result_payload,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }


def _job_matches_cep(job: OperationJob, cep: str | None):
    if not cep:
        return True
    payload = job.payload or {}
    return payload.get("cep") == cep


def list_operation_jobs(db, limit: int = 50, offset: int = 0, cep: str | None = None):
    jobs_query = db.query(OperationJob).order_by(OperationJob.created_at.desc(), OperationJob.id.desc())
    jobs = jobs_query.all()
    if cep:
        jobs = [job for job in jobs if _job_matches_cep(job, cep)]
    jobs = jobs[offset : offset + limit]
    return [operation_job_payload(job) for job in jobs]


def get_operation_job(db, job_id: int, cep: str | None = None):
    job = db.query(OperationJob).filter(OperationJob.id == job_id).first()
    if not job or not _job_matches_cep(job, cep):
        return None
    return job


def enqueue_operation_job(db, *, job_type: str, requested_by: str, payload: dict | None = None):
    payload = payload or {}
    fingerprint = _payload_fingerprint(payload)
    active_jobs = (
        db.query(OperationJob)
        .filter_by(job_type=job_type, payload_fingerprint=fingerprint)
        .order_by(OperationJob.created_at.desc(), OperationJob.id.desc())
        .all()
    )
    existing = next(
        (
            job
            for job in active_jobs
            if job.status in {"queued", "processing"}
        ),
        None,
    )

    now = datetime.now(UTC).replace(tzinfo=None)
    if existing:
        existing.request_count += 1
        existing.updated_at = now
        db.commit()
        db.refresh(existing)
        log_event(
            LOGGER,
            logging.INFO,
            "operation_job_reused",
            operation_job_id=existing.id,
            job_type=job_type,
            requested_by=requested_by,
            request_count=existing.request_count,
        )
        return existing

    job = OperationJob(
        job_type=job_type,
        requested_by=requested_by,
        status="queued",
        request_count=1,
        payload=payload,
        payload_fingerprint=fingerprint,
        created_at=now,
        updated_at=now,
    )
    db.add(job)
    db.commit()
    db.refresh(job)
    log_event(
        LOGGER,
        logging.INFO,
        "operation_job_enqueued",
        operation_job_id=job.id,
        job_type=job_type,
        requested_by=requested_by,
        payload=payload,
    )
    return job


def operation_job_metrics(db, cep: str | None = None):
    if cep:
        jobs = [job for job in db.query(OperationJob).all() if _job_matches_cep(job, cep)]
        queued = [job for job in jobs if job.status == "queued"]
        processing = [job for job in jobs if job.status == "processing"]
        completed = [job for job in jobs if job.status == "completed"]
        failed = [job for job in jobs if job.status == "failed"]
        oldest_queued = min((job.created_at for job in queued), default=None)
        oldest_queued_age_minutes = None
        if oldest_queued:
            oldest_queued_age_minutes = max(
                int((datetime.now(UTC).replace(tzinfo=None) - oldest_queued).total_seconds() // 60),
                0,
            )
        return {
            "total_jobs": len(jobs),
            "queued_jobs": len(queued),
            "processing_jobs": len(processing),
            "completed_jobs": len(completed),
            "failed_jobs": len(failed),
            "oldest_queued_job_minutes": oldest_queued_age_minutes,
        }
    try:
        status_rows = db.query(OperationJob.status, func.count(OperationJob.id).label("count")).group_by(OperationJob.status).all()
        counts = {
            getattr(row, "status", row[0]): int(getattr(row, "count", row[1]) or 0)
            for row in status_rows
        }
        oldest_queued_row = (
            db.query(func.min(OperationJob.created_at).label("oldest_created_at"))
            .filter(OperationJob.status == "queued")
            .first()
        )
        oldest_queued = None
        if oldest_queued_row:
            oldest_queued = getattr(oldest_queued_row, "oldest_created_at", oldest_queued_row[0])
        oldest_queued_age_minutes = None
        if oldest_queued:
            oldest_queued_age_minutes = max(
                int((datetime.now(UTC).replace(tzinfo=None) - oldest_queued).total_seconds() // 60),
                0,
            )
        return {
            "total_jobs": sum(counts.values()),
            "queued_jobs": counts.get("queued", 0),
            "processing_jobs": counts.get("processing", 0),
            "completed_jobs": counts.get("completed", 0),
            "failed_jobs": counts.get("failed", 0),
            "oldest_queued_job_minutes": oldest_queued_age_minutes,
        }
    except (AttributeError, TypeError, AssertionError):
        jobs = db.query(OperationJob).all()
        queued = [job for job in jobs if job.status == "queued"]
        processing = [job for job in jobs if job.status == "processing"]
        completed = [job for job in jobs if job.status == "completed"]
        failed = [job for job in jobs if job.status == "failed"]
        oldest_queued = min((job.created_at for job in queued), default=None)
        oldest_queued_age_minutes = None
        if oldest_queued:
            oldest_queued_age_minutes = max(
                int((datetime.now(UTC).replace(tzinfo=None) - oldest_queued).total_seconds() // 60),
                0,
            )

        return {
            "total_jobs": len(jobs),
            "queued_jobs": len(queued),
            "processing_jobs": len(processing),
            "completed_jobs": len(completed),
            "failed_jobs": len(failed),
            "oldest_queued_job_minutes": oldest_queued_age_minutes,
        }


def _search_job_result_payload(job: SearchJob | None, *, requested_job_id: int | None = None):
    if not job:
        if requested_job_id is not None:
            raise ValueError("Search job nao encontrado")
        return {"message": "Nenhum search job pendente na fila."}
    return {
        "search_job_id": job.id,
        "status": job.status,
        "query": job.query,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }


def _default_executors():
    from src.services.operational_cycle import run_operational_cycle
    from src.services.scheduled_collection import run_scheduled_collection
    from src.services.search_jobs import process_next_search_job, process_search_job

    return {
        JOB_TYPE_SCHEDULED_COLLECTION: lambda payload: run_scheduled_collection(payload.get("cep")),
        JOB_TYPE_OPERATIONAL_CYCLE: lambda payload: run_operational_cycle(
            cep=payload.get("cep"),
            force_collection=bool(payload.get("force_collection", False)),
        ),
        JOB_TYPE_PROCESS_NEXT_SEARCH_JOB: lambda payload: _search_job_result_payload(process_next_search_job()),
        JOB_TYPE_PROCESS_SEARCH_JOB: lambda payload: _search_job_result_payload(
            process_search_job(int(payload["search_job_id"])),
            requested_job_id=int(payload["search_job_id"]),
        ),
    }


def _complete_operation_job(session, job: OperationJob, *, status: str, result_payload=None, error_message: str | None = None):
    now = datetime.now(UTC).replace(tzinfo=None)
    job.status = status
    job.updated_at = now
    job.finished_at = now
    job.result_payload = _json_safe(result_payload)
    job.error_message = error_message
    session.commit()
    session.refresh(job)
    return job


def process_operation_job(job_id: int | None = None, *, session_factory=None, executors: dict | None = None):
    session_factory = session_factory or SessionLocal
    executors = executors or _default_executors()
    with session_factory() as session:
        query = session.query(OperationJob)
        if job_id is not None:
            query = query.filter(OperationJob.id == job_id)
        else:
            query = query.filter(OperationJob.status == "queued").order_by(OperationJob.created_at.asc(), OperationJob.id.asc())

        job = query.first()
        if not job:
            return None

        if job.status not in {"queued", "processing"}:
            return job

        executor = executors.get(job.job_type)
        if executor is None:
            return _complete_operation_job(
                session,
                job,
                status="failed",
                error_message=f"Tipo de operation job nao suportado: {job.job_type}",
            )

        now = datetime.now(UTC).replace(tzinfo=None)
        job.status = "processing"
        job.started_at = job.started_at or now
        job.updated_at = now
        session.commit()
        session.refresh(job)
        log_event(
            LOGGER,
            logging.INFO,
            "operation_job_processing_started",
            operation_job_id=job.id,
            job_type=job.job_type,
            payload=job.payload,
        )

        try:
            result_payload = executor(job.payload or {})
            completed_job = _complete_operation_job(session, job, status="completed", result_payload=result_payload)
            log_event(
                LOGGER,
                logging.INFO,
                "operation_job_processing_completed",
                operation_job_id=completed_job.id,
                job_type=completed_job.job_type,
            )
            return completed_job
        except Exception as exc:
            session.rollback()
            failed_job = _complete_operation_job(session, job, status="failed", error_message=str(exc)[:500])
            log_event(
                LOGGER,
                logging.ERROR,
                "operation_job_processing_failed",
                operation_job_id=failed_job.id,
                job_type=failed_job.job_type,
                error_message=failed_job.error_message,
            )
            return failed_job


def process_next_operation_job(*, session_factory=None, executors: dict | None = None):
    return process_operation_job(session_factory=session_factory, executors=executors)
