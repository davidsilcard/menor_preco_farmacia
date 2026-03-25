from pathlib import Path
import sys
import logging

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload, selectinload

if __package__ in {None, ""}:
    project_root = Path(__file__).resolve().parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

from src.api.catalog_routes import router as catalog_router
from src.api.deps import get_db
from src.api.ops_routes import router as ops_router
from src.core.config import settings
from src.core.logging import configure_logging
from src.core.logging import get_logger, log_event
from src.init_db import init_db as ensure_database_schema
from src.models.base import CanonicalProduct, PriceSnapshot, ProductMatch, SourceProduct
from src.services.catalog_queries import (
    build_latest_price_map,
    canonical_offer_payload,
    snapshot_freshness_payload,
    validate_cep_context,
)
from src.services.ops import ops_health_payload as _ops_health_payload
from src.services.ops import pharmacy_metrics as _pharmacy_metrics
from src.services.tool_models import (
    InvoiceComparisonRequest,
    ObservedItemRequest,
    ReceiptComparisonRequest,
    ShoppingListRequest,
)
from src.services.tool_use import (
    compare_basket_service,
    compare_canonical_product_service,
    compare_invoice_items_service,
    compare_receipt_service,
    compare_shopping_list_service,
    list_review_matches_service,
    search_observed_item_service,
    search_products_service,
)

configure_logging()
app = FastAPI(title="Monitor de Precos Jaragua do Sul")
app.include_router(catalog_router)
app.include_router(ops_router)
LOGGER = get_logger(__name__)


@app.on_event("startup")
def _ensure_database_schema_on_startup():
    ensure_database_schema()


def _validated_optional_cep(cep):
    return validate_cep_context(cep) if isinstance(cep, str) and cep else None


@app.get("/")
def read_root():
    return {
        "message": "Monitor de Precos de Farmacias Ativo",
        "regiao": "Jaragua do Sul - SC",
        "active_cep": settings.CEP,
        "model": "source_product + canonical_product + price_snapshot",
        "comparison_endpoints": ["/comparison/canonical-products", "/comparison/canonical/{id}"],
        "health_endpoints": ["/health/live", "/health/ready"],
        "operational_endpoints": [
            "/catalog/requests",
            "/search-jobs",
            "/search-jobs/{job_id}",
            "/tracked-items",
            "/ops/collection-plan",
            "/ops/schedule",
            "/ops/health/scrapers",
            "/ops/health/pages",
            "/ops/collections/run",
            "/ops/cycle/run",
            "/ops/health",
            "/ops/metrics",
            "/ops/jobs",
            "/ops/jobs/{operation_job_id}",
            "/ops/scrape-runs",
            "/ops/search-jobs/process-next",
            "/ops/search-jobs/{job_id}/process",
        ],
        "tool_endpoints": [
            "/tool/search-products",
            "/tool/compare-shopping-list",
            "/tool/compare-invoice-items",
            "/tool/compare-receipt",
            "/tool/search-observed-item",
        ],
    }


@app.get("/products")
def list_source_products(
    cep: str | None = Query(None, min_length=8),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    normalized_cep = _validated_optional_cep(cep)
    query = db.query(SourceProduct).options(
        joinedload(SourceProduct.pharmacy),
        joinedload(SourceProduct.match).joinedload(ProductMatch.canonical_product),
    )
    if normalized_cep:
        source_product_ids = list(build_latest_price_map(db, normalized_cep).keys())
        if not source_product_ids:
            return []
        query = query.filter(SourceProduct.id.in_(source_product_ids))
    products = query.order_by(SourceProduct.id.desc()).offset(offset).limit(limit).all()

    return [
        {
            "id": product.id,
            "pharmacy": product.pharmacy.name,
            "source_sku": product.source_sku,
            "raw_name": product.raw_name,
            "normalized_name": product.normalized_name,
            "source_url": product.source_url,
            "dosage": product.dosage,
            "presentation": product.presentation,
            "pack_size": product.pack_size,
            "ean_gtin": product.ean_gtin,
            "canonical_product_id": product.match.canonical_product_id if product.match else None,
            "match_type": product.match.match_type if product.match else None,
            "match_confidence": product.match.confidence if product.match else None,
            "review_status": product.match.review_status if product.match else None,
        }
        for product in products
    ]


@app.get("/prices/{source_product_id}")
def get_product_prices(
    source_product_id: int,
    cep: str | None = Query(None, min_length=8),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    normalized_cep = _validated_optional_cep(cep)
    product = (
        db.query(SourceProduct)
        .options(joinedload(SourceProduct.pharmacy), joinedload(SourceProduct.match))
        .filter(SourceProduct.id == source_product_id)
        .first()
    )
    if not product:
        raise HTTPException(status_code=404, detail="Produto de origem nao encontrado")

    query = db.query(PriceSnapshot).filter(PriceSnapshot.source_product_id == source_product_id)
    if normalized_cep:
        query = query.filter(PriceSnapshot.cep == normalized_cep)
    prices = query.order_by(PriceSnapshot.captured_at.desc()).offset(offset).limit(limit).all()

    return {
        "source_product": {
            "id": product.id,
            "raw_name": product.raw_name,
            "source_sku": product.source_sku,
            "pharmacy": product.pharmacy.name,
            "canonical_product_id": product.match.canonical_product_id if product.match else None,
            "match_type": product.match.match_type if product.match else None,
            "match_confidence": product.match.confidence if product.match else None,
            "review_status": product.match.review_status if product.match else None,
            "review_notes": product.match.review_notes if product.match else None,
            "requested_cep": normalized_cep,
        },
        "history": [
            {
                "price": price.price,
                "captured_at": price.captured_at,
                "cep": price.cep,
                "availability": price.availability,
                "source_url": price.source_url,
                "data_freshness": snapshot_freshness_payload(price),
            }
            for price in prices
        ],
    }


@app.get("/canonical-products")
def list_canonical_products(
    cep: str | None = Query(None, min_length=8),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    normalized_cep = _validated_optional_cep(cep)
    query = db.query(CanonicalProduct)
    if normalized_cep:
        source_product_ids = list(build_latest_price_map(db, normalized_cep).keys())
        if not source_product_ids:
            return []
        canonical_ids = {
            match.canonical_product_id
            for match in db.query(ProductMatch).filter(ProductMatch.source_product_id.in_(source_product_ids)).all()
            if match.canonical_product_id
        }
        if not canonical_ids:
            return []
        query = query.filter(CanonicalProduct.id.in_(canonical_ids))
    products = query.order_by(CanonicalProduct.id.desc()).offset(offset).limit(limit).all()
    return [
        {
            "id": product.id,
            "canonical_name": product.canonical_name,
            "normalized_name": product.normalized_name,
            "ean_gtin": product.ean_gtin,
            "anvisa_code": product.anvisa_code,
            "dosage": product.dosage,
            "presentation": product.presentation,
            "pack_size": product.pack_size,
        }
        for product in products
    ]


@app.get("/comparison/canonical-products")
def compare_canonical_products(
    cep: str = Query(..., min_length=8),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    min_matches: int = Query(2, ge=2, le=20),
    db: Session = Depends(get_db),
):
    normalized_cep = validate_cep_context(cep)
    canonical_ids = [
        canonical_id
        for canonical_id, in (
            db.query(CanonicalProduct.id)
            .join(CanonicalProduct.matches)
            .group_by(CanonicalProduct.id)
            .having(func.count(ProductMatch.id) >= min_matches)
            .order_by(CanonicalProduct.id.desc())
            .offset(offset)
            .limit(limit)
            .all()
        )
    ]
    if not canonical_ids:
        return []

    canonical_products = (
        db.query(CanonicalProduct)
        .options(
            selectinload(CanonicalProduct.matches)
            .selectinload(ProductMatch.source_product)
            .selectinload(SourceProduct.pharmacy)
        )
        .filter(CanonicalProduct.id.in_(canonical_ids))
        .all()
    )
    products_by_id = {product.id: product for product in canonical_products}
    latest_prices = build_latest_price_map(db, normalized_cep)

    results = []
    for canonical_id in canonical_ids:
        canonical_product = products_by_id.get(canonical_id)
        if not canonical_product:
            continue
        offers = canonical_offer_payload(canonical_product, latest_prices)
        if len(offers) < 2:
            continue
        results.append(
            {
                "canonical_product_id": canonical_product.id,
                "canonical_name": canonical_product.canonical_name,
                "ean_gtin": canonical_product.ean_gtin,
                "anvisa_code": canonical_product.anvisa_code,
                "lowest_price": offers[0],
                "offers": offers,
            }
        )

    return results


@app.get("/comparison/canonical/{canonical_product_id}")
def compare_single_canonical_product(
    canonical_product_id: int,
    cep: str = Query(..., min_length=8),
    db: Session = Depends(get_db),
):
    try:
        return compare_canonical_product_service(canonical_product_id, cep, db)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/matching/review")
def list_pending_reviews(
    cep: str | None = Query(None, min_length=8),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    return list_review_matches_service(db, cep=_validated_optional_cep(cep), limit=limit, offset=offset)


@app.get("/tool/search-products")
def tool_search_products(
    query: str = Query(..., min_length=2),
    cep: str = Query(..., min_length=8),
    db: Session = Depends(get_db),
):
    try:
        return search_products_service(query, cep, db)
    except HTTPException:
        raise
    except Exception as exc:
        log_event(
            LOGGER,
            logging.ERROR,
            "tool_search_products_failed",
            query=query,
            cep=cep,
            error_message=str(exc)[:500],
        )
        raise HTTPException(status_code=500, detail="Internal Server Error") from exc


@app.post("/tool/compare-shopping-list")
def tool_compare_shopping_list(payload: ShoppingListRequest = Body(...), db: Session = Depends(get_db)):
    return compare_shopping_list_service(payload, db)


@app.post("/tool/compare-basket")
def tool_compare_basket(payload: ShoppingListRequest = Body(...), db: Session = Depends(get_db)):
    return compare_basket_service(payload, db)


@app.post("/tool/compare-invoice-items")
def tool_compare_invoice_items(payload: InvoiceComparisonRequest = Body(...), db: Session = Depends(get_db)):
    return compare_invoice_items_service(payload, db)


@app.post("/tool/compare-receipt")
def tool_compare_receipt(payload: ReceiptComparisonRequest = Body(...), db: Session = Depends(get_db)):
    return compare_receipt_service(payload, db)


@app.post("/tool/search-observed-item")
def tool_search_observed_item(payload: ObservedItemRequest = Body(...), db: Session = Depends(get_db)):
    return search_observed_item_service(payload, db)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=settings.PORT,
        access_log=settings.UVICORN_ACCESS_LOG,
    )
