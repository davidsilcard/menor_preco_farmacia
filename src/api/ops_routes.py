from fastapi import APIRouter, HTTPException, Query

from src.models.base import ScrapeRun, SearchJob, SessionLocal
from src.services.catalog_queries import normalize_cep
from src.services.demand_tracking import search_job_payload
from src.services.ops import ops_health_payload, ops_metrics_payload, scrape_run_payload
from src.services.operational_cycle import collection_schedule_status, run_operational_cycle

router = APIRouter()


@router.get("/ops/collection-plan")
def get_collection_plan(cep: str | None = Query(None)):
    from src.services.scheduled_collection import build_scheduled_collection_plan

    normalized_cep = normalize_cep(cep) if cep else None
    return build_scheduled_collection_plan(normalized_cep)


@router.get("/ops/schedule")
def get_collection_schedule():
    return collection_schedule_status()


@router.get("/ops/health")
def ops_health():
    with SessionLocal() as db:
        return ops_health_payload(db)


@router.get("/ops/scrape-runs")
def list_scrape_runs(limit: int = Query(20, ge=1, le=200)):
    with SessionLocal() as db:
        runs = (
            db.query(ScrapeRun)
            .order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc())
            .limit(limit)
            .all()
        )
        return [scrape_run_payload(run) for run in runs]


@router.get("/ops/metrics")
def ops_metrics():
    with SessionLocal() as db:
        return ops_metrics_payload(db)


@router.post("/ops/search-jobs/process-next")
def process_next_search_job_endpoint():
    from src.services.search_jobs import process_next_search_job

    job = process_next_search_job()
    if not job:
        return {"message": "Nenhum search job pendente na fila."}
    with SessionLocal() as db:
        refreshed = db.query(SearchJob).filter(SearchJob.id == job.id).first()
        return search_job_payload(refreshed, db)


@router.post("/ops/collections/run")
def run_scheduled_collection_endpoint(cep: str | None = Query(None)):
    from src.services.scheduled_collection import run_scheduled_collection

    normalized_cep = normalize_cep(cep) if cep else None
    return run_scheduled_collection(normalized_cep)


@router.post("/ops/cycle/run")
def run_operational_cycle_endpoint(
    cep: str | None = Query(None),
    force_collection: bool = Query(False),
):
    normalized_cep = normalize_cep(cep) if cep else None
    return run_operational_cycle(cep=normalized_cep, force_collection=force_collection)


@router.post("/ops/search-jobs/{job_id}/process")
def process_search_job_endpoint(job_id: int):
    from src.services.search_jobs import process_search_job

    job = process_search_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Search job nao encontrado")
    with SessionLocal() as db:
        refreshed = db.query(SearchJob).filter(SearchJob.id == job.id).first()
        return search_job_payload(refreshed, db)
