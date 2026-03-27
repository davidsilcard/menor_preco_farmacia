from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.api.internal_auth import require_internal_api_auth
from src.models.base import ScrapeRun, SearchJob, SessionLocal
from src.services.catalog_queries import normalize_cep
from src.services.external_health import page_health_payload, scraper_health_payload
from src.services.operation_jobs import (
    JOB_TYPE_OPERATIONAL_CYCLE,
    JOB_TYPE_PROCESS_NEXT_SEARCH_JOB,
    JOB_TYPE_PROCESS_SEARCH_JOB,
    JOB_TYPE_SCHEDULED_COLLECTION,
    enqueue_operation_job,
    get_operation_job,
    list_operation_jobs,
    operation_job_payload,
)
from src.services.ops import (
    live_health_payload,
    ops_health_payload,
    ops_metrics_payload,
    readiness_health_payload,
    scrape_run_payload,
)
from src.services.operational_cycle import collection_schedule_status

router = APIRouter()
internal_router = APIRouter(dependencies=[Depends(require_internal_api_auth)])


@router.get("/health/live")
def health_live():
    return live_health_payload()


@internal_router.get("/health/ready")
def health_ready(response: Response):
    with SessionLocal() as db:
        payload = readiness_health_payload(db)
    if payload["status"] != "ready":
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return payload


@internal_router.get("/ops/collection-plan")
def get_collection_plan(cep: str | None = Query(None)):
    from src.services.scheduled_collection import build_scheduled_collection_plan

    normalized_cep = normalize_cep(cep) if cep else None
    return build_scheduled_collection_plan(normalized_cep)


@internal_router.get("/ops/schedule")
def get_collection_schedule():
    return collection_schedule_status()


@internal_router.get("/ops/health")
def ops_health(cep: str | None = Query(None)):
    with SessionLocal() as db:
        return ops_health_payload(db, normalize_cep(cep) if cep else None)


@internal_router.get("/ops/health/scrapers")
def ops_scraper_health(cep: str | None = Query(None)):
    with SessionLocal() as db:
        normalized_cep = normalize_cep(cep) if cep else None
        return scraper_health_payload(db, cep=normalized_cep)


@internal_router.get("/ops/health/pages")
def ops_page_health(
    sample_term: str = Query("dipirona", min_length=2),
    timeout_seconds: int = Query(8, ge=1, le=30),
):
    return page_health_payload(sample_term=sample_term, timeout_seconds=timeout_seconds)


@internal_router.get("/ops/scrape-runs")
def list_scrape_runs(
    cep: str | None = Query(None),
    limit: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    with SessionLocal() as db:
        query = db.query(ScrapeRun).order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc())
        if cep:
            query = query.filter(ScrapeRun.cep == normalize_cep(cep))
        runs = query.offset(offset).limit(limit).all()
        return [scrape_run_payload(run) for run in runs]


@internal_router.get("/ops/metrics")
def ops_metrics(cep: str | None = Query(None)):
    with SessionLocal() as db:
        return ops_metrics_payload(db, normalize_cep(cep) if cep else None)


@internal_router.get("/ops/jobs")
def get_operation_jobs(
    cep: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    return list_operation_jobs(db, limit=limit, offset=offset, cep=normalize_cep(cep) if cep else None)


@internal_router.get("/ops/jobs/{operation_job_id}")
def get_operation_job_endpoint(operation_job_id: int, cep: str | None = Query(None), db: Session = Depends(get_db)):
    job = get_operation_job(db, operation_job_id, cep=normalize_cep(cep) if cep else None)
    if not job:
        raise HTTPException(status_code=404, detail="Operation job nao encontrado")
    return operation_job_payload(job)


@internal_router.post("/ops/search-jobs/process-next", status_code=status.HTTP_202_ACCEPTED)
def process_next_search_job_endpoint(db: Session = Depends(get_db)):
    job = enqueue_operation_job(
        db,
        job_type=JOB_TYPE_PROCESS_NEXT_SEARCH_JOB,
        requested_by="ops_api",
        payload={},
    )
    return operation_job_payload(job)


@internal_router.post("/ops/collections/run", status_code=status.HTTP_202_ACCEPTED)
def run_scheduled_collection_endpoint(cep: str | None = Query(None), db: Session = Depends(get_db)):
    normalized_cep = normalize_cep(cep) if cep else None
    job = enqueue_operation_job(
        db,
        job_type=JOB_TYPE_SCHEDULED_COLLECTION,
        requested_by="ops_api",
        payload={"cep": normalized_cep},
    )
    return operation_job_payload(job)


@internal_router.post("/ops/cycle/run", status_code=status.HTTP_202_ACCEPTED)
def run_operational_cycle_endpoint(
    cep: str | None = Query(None),
    force_collection: bool = Query(False),
    db: Session = Depends(get_db),
):
    normalized_cep = normalize_cep(cep) if cep else None
    job = enqueue_operation_job(
        db,
        job_type=JOB_TYPE_OPERATIONAL_CYCLE,
        requested_by="ops_api",
        payload={"cep": normalized_cep, "force_collection": force_collection},
    )
    return operation_job_payload(job)


@internal_router.post("/ops/search-jobs/{job_id}/process", status_code=status.HTTP_202_ACCEPTED)
def process_search_job_endpoint(job_id: int, db: Session = Depends(get_db)):
    search_job = db.query(SearchJob).filter(SearchJob.id == job_id).first()
    if not search_job:
        raise HTTPException(status_code=404, detail="Search job nao encontrado")
    job = enqueue_operation_job(
        db,
        job_type=JOB_TYPE_PROCESS_SEARCH_JOB,
        requested_by="ops_api",
        payload={"search_job_id": job_id, "cep": search_job.cep},
    )
    return operation_job_payload(job)


router.include_router(internal_router)
