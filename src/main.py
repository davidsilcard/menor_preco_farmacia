from fastapi import Body, Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

from src.api.catalog_routes import router as catalog_router
from src.api.deps import get_db
from src.api.ops_routes import router as ops_router
from src.core.config import settings
from src.models.base import CanonicalProduct, PriceSnapshot, ProductMatch, SourceProduct
from src.services.catalog_queries import (
    build_latest_price_map,
    canonical_offer_payload,
    snapshot_freshness_payload,
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

app = FastAPI(title="Monitor de Precos Jaragua do Sul")
app.include_router(catalog_router)
app.include_router(ops_router)


@app.get("/")
def read_root():
    return {
        "message": "Monitor de Precos de Farmacias Ativo",
        "regiao": "Jaragua do Sul - SC",
        "active_cep": settings.CEP,
        "model": "source_product + canonical_product + price_snapshot",
        "comparison_endpoints": ["/comparison/canonical-products", "/comparison/canonical/{id}"],
        "operational_endpoints": [
            "/catalog/requests",
            "/search-jobs",
            "/search-jobs/{job_id}",
            "/tracked-items",
            "/ops/collection-plan",
            "/ops/schedule",
            "/ops/collections/run",
            "/ops/cycle/run",
            "/ops/health",
            "/ops/metrics",
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
def list_source_products(db: Session = Depends(get_db)):
    products = (
        db.query(SourceProduct)
        .options(
            joinedload(SourceProduct.pharmacy),
            joinedload(SourceProduct.match).joinedload(ProductMatch.canonical_product),
        )
        .all()
    )

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
def get_product_prices(source_product_id: int, db: Session = Depends(get_db)):
    product = (
        db.query(SourceProduct)
        .options(joinedload(SourceProduct.pharmacy), joinedload(SourceProduct.match))
        .filter(SourceProduct.id == source_product_id)
        .first()
    )
    if not product:
        raise HTTPException(status_code=404, detail="Produto de origem nao encontrado")

    prices = (
        db.query(PriceSnapshot)
        .filter(PriceSnapshot.source_product_id == source_product_id)
        .order_by(PriceSnapshot.captured_at.desc())
        .all()
    )

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
def list_canonical_products(db: Session = Depends(get_db)):
    products = db.query(CanonicalProduct).all()
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
def compare_canonical_products(db: Session = Depends(get_db)):
    canonical_products = (
        db.query(CanonicalProduct)
        .options(
            joinedload(CanonicalProduct.matches)
            .joinedload(ProductMatch.source_product)
            .joinedload(SourceProduct.pharmacy)
        )
        .all()
    )
    latest_prices = build_latest_price_map(db)

    results = []
    for canonical_product in canonical_products:
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
def compare_single_canonical_product(canonical_product_id: int, db: Session = Depends(get_db)):
    try:
        return compare_canonical_product_service(canonical_product_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/matching/review")
def list_pending_reviews(db: Session = Depends(get_db)):
    return list_review_matches_service(db)


@app.get("/tool/search-products")
def tool_search_products(
    query: str = Query(..., min_length=2),
    cep: str = Query(..., min_length=8),
    db: Session = Depends(get_db),
):
    return search_products_service(query, cep, db)


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

    uvicorn.run(app, host="0.0.0.0", port=settings.PORT)
