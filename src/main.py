import re

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, joinedload

from src.core.config import settings
from src.models.base import CanonicalProduct, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper

app = FastAPI(title="Monitor de Precos Jaragua do Sul")

SEARCH_TERM_ALIASES = {
    "cpr": "comprimidos",
    "comp": "comprimidos",
    "comps": "comprimidos",
    "caps": "capsulas",
    "cap": "capsulas",
    "sol": "solucao",
    "sol oral": "solucao oral",
    "gts": "gotas",
    "susp": "suspensao",
    "inj": "injetavel",
    "inf": "infantil",
    "gen": "generico",
    "dip sod": "dipirona sodica",
    "dip mono": "dipirona monoidratada",
}

SEARCH_STOPWORDS = {
    "cx",
    "cxs",
    "und",
    "un",
    "unid",
    "unidades",
    "frasco",
    "caixa",
    "blister",
    "preco",
    "valor",
    "loja",
    "farmacia",
    "farmacias",
}


class ShoppingListRequest(BaseModel):
    cep: str
    items: list[str] = Field(default_factory=list)


class InvoiceItemInput(BaseModel):
    description: str
    paid_price: float | None = None
    quantity: int | None = 1


class InvoiceComparisonRequest(BaseModel):
    cep: str
    items: list[InvoiceItemInput] = Field(default_factory=list)


class ReceiptComparisonRequest(BaseModel):
    cep: str
    items: list[InvoiceItemInput] = Field(default_factory=list)
    merchant_name: str | None = None
    captured_at: str | None = None


class ObservedItemRequest(BaseModel):
    cep: str
    observations: list[str] = Field(default_factory=list)
    source_type: str = "free_text"


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {
        "message": "Monitor de Precos de Farmacias Ativo",
        "regiao": "Jaragua do Sul - SC",
        "active_cep": settings.CEP,
        "model": "source_product + canonical_product + price_snapshot",
        "comparison_endpoints": ["/comparison/canonical-products", "/comparison/canonical/{id}"],
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


def _build_latest_price_map(db: Session):
    snapshots = (
        db.query(PriceSnapshot)
        .order_by(PriceSnapshot.source_product_id.asc(), PriceSnapshot.captured_at.desc())
        .all()
    )
    latest_by_source_product = {}
    for snapshot in snapshots:
        latest_by_source_product.setdefault(snapshot.source_product_id, snapshot)
    return latest_by_source_product


def _normalize_cep(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def _validate_cep_context(cep: str):
    requested_cep = _normalize_cep(cep)
    active_cep = _normalize_cep(settings.CEP)
    if not requested_cep:
        raise HTTPException(status_code=400, detail="CEP e obrigatorio para consultar preços por regiao.")
    if requested_cep != active_cep:
        raise HTTPException(
            status_code=409,
            detail=f"Os dados atuais foram coletados para o CEP {settings.CEP}. Recolete os scrapers para o CEP solicitado.",
        )
    return requested_cep


def _normalize_query(value: str) -> str:
    normalized = BaseScraper.normalize_text(value or "")
    normalized = re.sub(r"r\$\s*\d+[.,]?\d*", " ", normalized)
    normalized = re.sub(r"\b\d{4,}\b", lambda match: match.group(0), normalized)
    normalized = re.sub(r"[^a-z0-9\s/.,-]", " ", normalized)

    for alias, expanded in SEARCH_TERM_ALIASES.items():
        normalized = re.sub(rf"\b{re.escape(alias)}\b", expanded, normalized)

    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _tokenize_search_text(value: str):
    normalized = _normalize_query(value)
    tokens = []
    for token in normalized.split():
        if token in SEARCH_STOPWORDS:
            continue
        if len(token) == 1 and not token.isdigit():
            continue
        tokens.append(token)
    return tokens


def _canonical_offer_payload(canonical_product: CanonicalProduct, latest_prices: dict):
    offers = []
    for match in canonical_product.matches:
        source_product = match.source_product
        latest_snapshot = latest_prices.get(source_product.id)
        if not latest_snapshot:
            continue
        offers.append(
            {
                "source_product_id": source_product.id,
                "pharmacy": source_product.pharmacy.name,
                "raw_name": source_product.raw_name,
                "source_sku": source_product.source_sku,
                "price": latest_snapshot.price,
                "captured_at": latest_snapshot.captured_at,
                "availability": latest_snapshot.availability,
                "source_url": latest_snapshot.source_url,
                "ean_gtin": source_product.ean_gtin,
                "anvisa_code": source_product.anvisa_code,
                "match_type": match.match_type,
                "match_confidence": match.confidence,
                "review_status": match.review_status,
                "review_notes": match.review_notes,
            }
        )

    offers.sort(key=lambda offer: offer["price"])
    return offers


def _score_canonical_match(canonical_product: CanonicalProduct, query: str):
    normalized_query = _normalize_query(query)
    tokens = _tokenize_search_text(query)
    source_aliases = " ".join(match.source_product.normalized_name for match in canonical_product.matches if match.source_product)
    haystack = " ".join(
        filter(
            None,
            [
                canonical_product.normalized_name,
                source_aliases,
                canonical_product.ean_gtin,
                canonical_product.anvisa_code,
                canonical_product.brand,
                canonical_product.active_ingredient,
                canonical_product.dosage,
                canonical_product.pack_size,
            ],
        )
    ).lower()

    score = 0
    if canonical_product.ean_gtin and normalized_query == canonical_product.ean_gtin:
        score += 100
    if canonical_product.anvisa_code and normalized_query == canonical_product.anvisa_code:
        score += 95
    if canonical_product.normalized_name == normalized_query:
        score += 70

    for token in tokens:
        if token.isdigit() and len(token) >= 8:
            continue
        if token in haystack:
            if token.endswith(("mg", "ml", "g")) or "x" in token:
                score += 15
            else:
                score += 10

    if canonical_product.dosage and _normalize_query(canonical_product.dosage) in normalized_query:
        score += 20
    if canonical_product.pack_size and _normalize_query(canonical_product.pack_size) in normalized_query:
        score += 20
    if canonical_product.brand and _normalize_query(canonical_product.brand) in normalized_query:
        score += 15

    strong_digits = re.findall(r"\b\d{8,14}\b", normalized_query)
    for digits in strong_digits:
        if canonical_product.ean_gtin == digits:
            score += 100
        if canonical_product.anvisa_code == digits:
            score += 95

    return score


def _find_matching_canonicals(db: Session, query: str, limit: int = 5):
    canonical_products = (
        db.query(CanonicalProduct)
        .options(
            joinedload(CanonicalProduct.matches)
            .joinedload(ProductMatch.source_product)
            .joinedload(SourceProduct.pharmacy)
        )
        .all()
    )

    ranked = []
    for canonical_product in canonical_products:
        score = _score_canonical_match(canonical_product, query)
        if score > 0:
            ranked.append((score, canonical_product))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[:limit]


def _tool_response(tool_name: str, tool_input: dict, result, confidence: float, warnings: list[str] | None = None):
    return {
        "tool_name": tool_name,
        "input": tool_input,
        "confidence": round(confidence, 2),
        "warnings": warnings or [],
        "result": result,
    }


def _estimate_overall_confidence(items: list[dict]):
    scores = [item.get("score", 0) for item in items if item.get("match_found")]
    if not scores:
        return 0.0
    return min((sum(scores) / len(scores)) / 100, 1.0)


def _build_price_summary(items: list[dict]):
    total_paid = round(sum((item.get("paid_price") or 0) * (item.get("quantity") or 1) for item in items), 2)
    total_best_price = round(
        sum((item.get("best_offer", {}).get("price") or 0) * (item.get("quantity") or 1) for item in items if item.get("best_offer")),
        2,
    )
    total_potential_savings = round(sum(item.get("potential_savings") or 0 for item in items), 2)

    pharmacy_totals = {}
    for item in items:
        quantity = item.get("quantity") or 1
        for offer in item.get("offers", []):
            pharmacy_totals.setdefault(offer["pharmacy"], 0)
            pharmacy_totals[offer["pharmacy"]] += round(offer["price"] * quantity, 2)

    pharmacy_totals = {key: round(value, 2) for key, value in pharmacy_totals.items()}
    best_basket_pharmacy = None
    if pharmacy_totals:
        best_name = min(pharmacy_totals, key=pharmacy_totals.get)
        best_basket_pharmacy = {"pharmacy": best_name, "estimated_total": pharmacy_totals[best_name]}

    return {
        "total_paid_informed": total_paid,
        "total_best_available": total_best_price,
        "total_potential_savings": total_potential_savings,
        "estimated_totals_by_pharmacy": pharmacy_totals,
        "best_basket_pharmacy": best_basket_pharmacy,
    }


def _build_observed_query(payload: ObservedItemRequest):
    joined = " ".join(payload.observations)
    query = _normalize_query(joined)
    query = re.sub(r"\b(lote|validade|fab|fabricacao|ind\.?|industria brasileira)\b.*", " ", query)
    query = re.sub(r"\s+", " ", query).strip()
    return query


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
    latest_prices = _build_latest_price_map(db)

    results = []
    for canonical_product in canonical_products:
        offers = _canonical_offer_payload(canonical_product, latest_prices)
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
    canonical_product = (
        db.query(CanonicalProduct)
        .options(
            joinedload(CanonicalProduct.matches)
            .joinedload(ProductMatch.source_product)
            .joinedload(SourceProduct.pharmacy)
        )
        .filter(CanonicalProduct.id == canonical_product_id)
        .first()
    )
    if not canonical_product:
        raise HTTPException(status_code=404, detail="Produto canonico nao encontrado")

    latest_prices = _build_latest_price_map(db)
    offers = _canonical_offer_payload(canonical_product, latest_prices)
    return {
        "canonical_product_id": canonical_product.id,
        "canonical_name": canonical_product.canonical_name,
        "ean_gtin": canonical_product.ean_gtin,
        "anvisa_code": canonical_product.anvisa_code,
        "offers": offers,
    }


@app.get("/matching/review")
def list_pending_reviews(db: Session = Depends(get_db)):
    products = (
        db.query(SourceProduct)
        .options(
            joinedload(SourceProduct.pharmacy),
            joinedload(SourceProduct.match).joinedload(ProductMatch.canonical_product),
        )
        .join(ProductMatch, ProductMatch.source_product_id == SourceProduct.id)
        .filter(ProductMatch.review_status == "needs_review")
        .all()
    )

    return [
        {
            "source_product_id": product.id,
            "pharmacy": product.pharmacy.name,
            "raw_name": product.raw_name,
            "ean_gtin": product.ean_gtin,
            "anvisa_code": product.anvisa_code,
            "canonical_product_id": product.match.canonical_product_id,
            "canonical_name": product.match.canonical_product.canonical_name if product.match and product.match.canonical_product else None,
            "match_type": product.match.match_type,
            "match_confidence": product.match.confidence,
            "review_notes": product.match.review_notes,
        }
        for product in products
    ]


@app.get("/tool/search-products")
def tool_search_products(
    query: str = Query(..., min_length=2),
    cep: str = Query(..., min_length=8),
    db: Session = Depends(get_db),
):
    _validate_cep_context(cep)
    latest_prices = _build_latest_price_map(db)
    matches = _find_matching_canonicals(db, query)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": _canonical_offer_payload(canonical_product, latest_prices),
        }
        for score, canonical_product in matches
    ]
    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    warnings = [] if results else ["Nenhum produto canonico encontrado para a consulta."]
    if results and confidence < 0.5:
        warnings.append("Match encontrado com baixa confianca; revisar item e ofertas.")

    return _tool_response(
        "search_products",
        {"query": query, "cep": cep},
        {"results": results},
        confidence,
        warnings,
    )


@app.post("/tool/compare-shopping-list")
def tool_compare_shopping_list(payload: ShoppingListRequest = Body(...), db: Session = Depends(get_db)):
    _validate_cep_context(payload.cep)
    latest_prices = _build_latest_price_map(db)
    comparisons = []
    max_score = 0

    for item in payload.items:
        matches = _find_matching_canonicals(db, item, limit=1)
        if not matches:
            comparisons.append(
                {
                    "requested_item": item,
                    "match_found": False,
                    "results": [],
                }
            )
            continue

        score, canonical_product = matches[0]
        max_score = max(max_score, score)
        offers = _canonical_offer_payload(canonical_product, latest_prices)
        comparisons.append(
            {
                "requested_item": item,
                "match_found": True,
                "score": score,
                "canonical_product_id": canonical_product.id,
                "canonical_name": canonical_product.canonical_name,
                "best_offer": offers[0] if offers else None,
                "offers": offers,
            }
        )

    unmatched_count = len([item for item in comparisons if not item["match_found"]])
    warnings = []
    if unmatched_count:
        warnings.append(f"{unmatched_count} item(ns) da lista nao tiveram match.")
    if comparisons and max_score < 50:
        warnings.append("Alguns matches da lista tem baixa confianca.")

    return _tool_response(
        "compare_shopping_list",
        payload.model_dump(),
        {"items": comparisons},
        min(max_score / 100, 1.0) if comparisons else 0.0,
        warnings,
    )


@app.post("/tool/compare-invoice-items")
def tool_compare_invoice_items(payload: InvoiceComparisonRequest = Body(...), db: Session = Depends(get_db)):
    _validate_cep_context(payload.cep)
    latest_prices = _build_latest_price_map(db)
    comparisons = []
    max_score = 0

    for item in payload.items:
        matches = _find_matching_canonicals(db, item.description, limit=1)
        if not matches:
            comparisons.append(
                {
                    "invoice_item": item.description,
                    "paid_price": item.paid_price,
                    "quantity": item.quantity,
                    "match_found": False,
                    "potential_savings": None,
                    "results": [],
                }
            )
            continue

        score, canonical_product = matches[0]
        max_score = max(max_score, score)
        offers = _canonical_offer_payload(canonical_product, latest_prices)
        best_offer = offers[0] if offers else None
        potential_savings = None
        if best_offer and item.paid_price is not None:
            potential_savings = round(max(item.paid_price - best_offer["price"], 0), 2)

        comparisons.append(
            {
                "invoice_item": item.description,
                "paid_price": item.paid_price,
                "quantity": item.quantity,
                "match_found": True,
                "score": score,
                "canonical_product_id": canonical_product.id,
                "canonical_name": canonical_product.canonical_name,
                "best_offer": best_offer,
                "potential_savings": potential_savings,
                "offers": offers,
            }
        )

    total_potential_savings = round(
        sum(item["potential_savings"] or 0 for item in comparisons),
        2,
    )
    unmatched_count = len([item for item in comparisons if not item["match_found"]])
    warnings = []
    if unmatched_count:
        warnings.append(f"{unmatched_count} item(ns) da nota nao tiveram match.")
    if comparisons and max_score < 50:
        warnings.append("Alguns matches da nota tem baixa confianca.")

    return _tool_response(
        "compare_invoice_items",
        payload.model_dump(),
        {
            "items": comparisons,
            "total_potential_savings": total_potential_savings,
        },
        min(max_score / 100, 1.0) if comparisons else 0.0,
        warnings,
    )


@app.post("/tool/compare-receipt")
def tool_compare_receipt(payload: ReceiptComparisonRequest = Body(...), db: Session = Depends(get_db)):
    _validate_cep_context(payload.cep)
    invoice_result = tool_compare_invoice_items(
        InvoiceComparisonRequest(cep=payload.cep, items=payload.items),
        db,
    )
    items = invoice_result["result"]["items"]
    summary = _build_price_summary(items)
    warnings = list(invoice_result["warnings"])
    if not items:
        warnings.append("Nenhum item foi enviado para comparacao da nota.")

    return _tool_response(
        "compare_receipt",
        payload.model_dump(),
        {
            "merchant_name": payload.merchant_name,
            "captured_at": payload.captured_at,
            "items": items,
            "summary": summary,
        },
        _estimate_overall_confidence(items),
        warnings,
    )


@app.post("/tool/search-observed-item")
def tool_search_observed_item(payload: ObservedItemRequest = Body(...), db: Session = Depends(get_db)):
    _validate_cep_context(payload.cep)
    query = _build_observed_query(payload)
    latest_prices = _build_latest_price_map(db)
    matches = _find_matching_canonicals(db, query)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": _canonical_offer_payload(canonical_product, latest_prices),
        }
        for score, canonical_product in matches
    ]

    warnings = []
    if payload.source_type == "box_photo":
        warnings.append("Entrada tratada como OCR de caixa; revise lote, validade e textos promocionais ignorados.")
    if not results:
        warnings.append("Nenhum produto canonico encontrado a partir das observacoes enviadas.")
    elif results[0]["score"] < 50:
        warnings.append("Match encontrado com baixa confianca para item observado.")

    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    return _tool_response(
        "search_observed_item",
        payload.model_dump(),
        {
            "normalized_query": query,
            "results": results,
        },
        confidence,
        warnings,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.PORT)
