from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.deps import get_db
from src.models.base import CatalogRequest, SearchJob, TrackedItemByCep
from src.services.catalog_queries import normalize_cep
from src.services.demand_tracking import catalog_request_payload, search_job_payload, tracked_item_payload

router = APIRouter()


@router.get("/catalog/requests")
def list_catalog_requests(db: Session = Depends(get_db)):
    requests = db.query(CatalogRequest).order_by(CatalogRequest.last_requested_at.desc()).all()
    return [catalog_request_payload(request) for request in requests]


@router.get("/search-jobs")
def list_search_jobs(db: Session = Depends(get_db)):
    jobs = db.query(SearchJob).order_by(SearchJob.created_at.desc(), SearchJob.id.desc()).all()
    return [search_job_payload(job, db) for job in jobs]


@router.get("/search-jobs/{job_id}")
def get_search_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(SearchJob).filter(SearchJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Search job nao encontrado")
    return search_job_payload(job, db)


@router.get("/tracked-items")
def list_tracked_items(
    cep: str | None = Query(None),
    status: str | None = Query(None),
    include_inactive: bool = Query(False),
    db: Session = Depends(get_db),
):
    query = db.query(TrackedItemByCep).order_by(
        TrackedItemByCep.scrape_priority.desc(),
        TrackedItemByCep.last_requested_at.desc(),
    )
    if cep:
        query = query.filter(TrackedItemByCep.cep == normalize_cep(cep))
    if status:
        query = query.filter(TrackedItemByCep.status == status)
    elif not include_inactive:
        query = query.filter(TrackedItemByCep.status != "inactive")

    items = query.all()
    return [tracked_item_payload(item) for item in items]
