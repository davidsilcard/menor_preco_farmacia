import re
from datetime import UTC, datetime

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session, joinedload

from src.core.config import settings
from src.models.base import CanonicalProduct, PriceSnapshot, ProductMatch, SessionLocal, SourceProduct
from src.scrapers.base import BaseScraper

app = FastAPI(title="Monitor de Precos Jaragua do Sul")

FRESH_DATA_MAX_AGE_MINUTES = 12 * 60
STALE_DATA_MAX_AGE_MINUTES = 24 * 60

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

SEARCH_SPECIAL_TOKENS = {
    "efervescentes",
    "efervescente",
    "gotas",
    "gota",
    "supositorio",
    "supositorios",
    "solucao",
    "oral",
    "flash",
    "seringa",
    "infantil",
}

STRICT_CANDIDATE_SPECIAL_TOKENS = {
    "efervescentes",
    "efervescente",
    "supositorio",
    "supositorios",
    "flash",
    "seringa",
    "infantil",
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
                "data_freshness": _snapshot_freshness_payload(price),
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


def _data_age_minutes(captured_at):
    if not captured_at:
        return None
    now_utc = datetime.now(UTC).replace(tzinfo=None)
    return max(int((now_utc - captured_at).total_seconds() // 60), 0)


def _freshness_status(captured_at):
    age_minutes = _data_age_minutes(captured_at)
    if age_minutes is None:
        return "unknown"
    if age_minutes <= FRESH_DATA_MAX_AGE_MINUTES:
        return "fresh"
    if age_minutes <= STALE_DATA_MAX_AGE_MINUTES:
        return "stale"
    return "expired"


def _snapshot_freshness_payload(snapshot: PriceSnapshot):
    return {
        "captured_at": snapshot.captured_at,
        "data_age_minutes": _data_age_minutes(snapshot.captured_at),
        "freshness_status": _freshness_status(snapshot.captured_at),
        "scrape_run_id": snapshot.scrape_run_id,
    }


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


def _significant_search_tokens(value: str):
    tokens = _tokenize_search_text(value)
    stopwords = {
        "analgesico",
        "antitermico",
        "adulto",
        "para",
        "de",
        "e",
        "monoidratada",
        "framboesa",
        "dipirona",
    }
    return {token for token in tokens if token not in stopwords}


def _has_special_token_conflict(query: str, candidate: str):
    normalized_query = _normalize_query(query)
    if re.fullmatch(r"\d{8,14}", normalized_query):
        return False

    query_tokens = _significant_search_tokens(query)
    candidate_tokens = _significant_search_tokens(candidate)
    query_special = {token for token in SEARCH_SPECIAL_TOKENS if token in query_tokens}
    candidate_special = {token for token in SEARCH_SPECIAL_TOKENS if token in candidate_tokens}

    if any(token not in candidate_special for token in query_special):
        return True

    if any(token in candidate_special and token not in query_special for token in STRICT_CANDIDATE_SPECIAL_TOKENS):
        return True

    return False


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
                "data_freshness": _snapshot_freshness_payload(latest_snapshot),
                "ean_gtin": source_product.ean_gtin,
                "anvisa_code": source_product.anvisa_code,
                "match_type": match.match_type,
                "match_confidence": match.confidence,
                "review_status": match.review_status,
                "review_notes": match.review_notes,
            }
        )

    offers.sort(key=lambda offer: (_availability_rank(offer.get("availability")), offer["price"]))
    return offers


def _pricing_eligible_offers(offers: list[dict]):
    return [offer for offer in offers if offer.get("availability") != "out_of_stock"]


def _best_pricing_offer(offers: list[dict]):
    eligible = _pricing_eligible_offers(offers)
    eligible.sort(key=lambda offer: (_availability_rank(offer.get("availability")), offer["price"]))
    return eligible[0] if eligible else None


def _availability_rank(availability: str | None):
    if availability == "available":
        return 0
    if availability == "unknown":
        return 1
    return 2


def _score_canonical_match(canonical_product: CanonicalProduct, query: str):
    normalized_query = _normalize_query(query)
    tokens = _tokenize_search_text(query)
    if _has_special_token_conflict(normalized_query, canonical_product.normalized_name):
        return 0

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

    query_tokens = _significant_search_tokens(normalized_query)
    candidate_tokens = _significant_search_tokens(canonical_product.normalized_name)
    overlap = query_tokens.intersection(candidate_tokens)
    if overlap:
        score += min(len(overlap) * 12, 36)
    if query_tokens and overlap == query_tokens:
        score += 20

    for token in tokens:
        if token.isdigit() and len(token) >= 8:
            continue
        if token in haystack:
            if token.endswith(("mg", "ml", "g")) or "x" in token:
                score += 15
            else:
                score += 10

    normalized_dosage = _normalize_query(canonical_product.dosage) if canonical_product.dosage else None
    if normalized_dosage and normalized_dosage in normalized_query:
        score += 20
    elif normalized_dosage:
        dosage_prefix = normalized_dosage.split("/")[0]
        if dosage_prefix and dosage_prefix in normalized_query:
            score += 18
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
    matched_items = [item for item in items if item.get("match_found")]
    priced_items = [item for item in matched_items if item.get("best_offer")]
    total_best_price = round(
        sum((item.get("best_offer", {}).get("price") or 0) * (item.get("quantity") or 1) for item in priced_items if item.get("best_offer")),
        2,
    )
    total_potential_savings = round(sum(item.get("potential_savings") or 0 for item in items), 2)

    candidate_pharmacies = sorted(
        {
            offer["pharmacy"]
            for item in matched_items
            for offer in item.get("offers", [])
        }
    )

    pharmacy_totals = {}
    unavailable_by_pharmacy = {}
    for pharmacy in candidate_pharmacies:
        total = 0.0
        unavailable_items = []
        for item in matched_items:
            quantity = item.get("quantity") or 1
            offer = next(
                (
                    offer
                    for offer in item.get("offers", [])
                    if offer["pharmacy"] == pharmacy and offer.get("availability") != "out_of_stock"
                ),
                None,
            )
            if not offer:
                unavailable_items.append(item.get("requested_item") or item.get("invoice_item"))
                continue
            total += offer["price"] * quantity
        if unavailable_items:
            unavailable_by_pharmacy[pharmacy] = unavailable_items
            continue
        pharmacy_totals[pharmacy] = round(total, 2)

    best_basket_pharmacy = None
    if pharmacy_totals:
        best_name = min(pharmacy_totals, key=pharmacy_totals.get)
        best_basket_pharmacy = {"pharmacy": best_name, "estimated_total": pharmacy_totals[best_name]}

    return {
        "total_paid_informed": total_paid,
        "total_best_available": total_best_price,
        "total_potential_savings": total_potential_savings,
        "matched_items": len(matched_items),
        "unmatched_items": len([item for item in items if not item.get("match_found")]),
        "estimated_totals_by_pharmacy": pharmacy_totals,
        "unavailable_items_by_pharmacy": unavailable_by_pharmacy,
        "best_basket_pharmacy": best_basket_pharmacy,
    }


def _build_basket_result(items: list[dict]):
    summary = _build_price_summary(items)
    return {
        "items": items,
        "summary": summary,
        "availability_summary": _basket_availability_summary(items),
        "data_freshness": _basket_freshness_summary(items),
    }


def _build_observed_query(payload: ObservedItemRequest):
    joined = " ".join(payload.observations)
    query = _normalize_query(joined)
    query = re.sub(r"\b(lote|validade|fab|fabricacao|ind\.?|industria brasileira)\b.*", " ", query)
    query = re.sub(r"\s+", " ", query).strip()
    return query


def _item_availability_state(item: dict):
    offers = item.get("offers", [])
    eligible = _pricing_eligible_offers(offers)
    if offers and not eligible:
        return "only_out_of_stock"
    if eligible and all(offer.get("availability") == "unknown" for offer in eligible):
        return "only_unknown"
    return None


def _item_availability_summary(item: dict):
    offers = item.get("offers", [])
    counts = {"available": 0, "unknown": 0, "out_of_stock": 0}
    for offer in offers:
        availability = offer.get("availability") or "unknown"
        counts[availability] = counts.get(availability, 0) + 1

    state = _item_availability_state(item)
    if item.get("match_found") and not offers:
        state = "no_offers"
    elif counts["available"] > 0:
        state = "has_available_offers"
    elif state == "only_unknown":
        state = "only_unknown_offers"
    elif state == "only_out_of_stock":
        state = "only_out_of_stock_offers"

    return {
        "state": state,
        "offer_counts": counts,
        "best_offer_availability": (item.get("best_offer") or {}).get("availability"),
    }


def _basket_availability_summary(items: list[dict]):
    item_summaries = [_item_availability_summary(item) for item in items if item.get("match_found")]
    return {
        "items_with_available_offers": sum(1 for summary in item_summaries if summary["state"] == "has_available_offers"),
        "items_only_unknown_offers": sum(1 for summary in item_summaries if summary["state"] == "only_unknown_offers"),
        "items_only_out_of_stock_offers": sum(1 for summary in item_summaries if summary["state"] == "only_out_of_stock_offers"),
        "items_without_offers": sum(1 for summary in item_summaries if summary["state"] == "no_offers"),
    }


def _basket_freshness_summary(items: list[dict]):
    best_offers = [item.get("best_offer") for item in items if item.get("best_offer")]
    freshness_payloads = [offer.get("data_freshness") for offer in best_offers if offer.get("data_freshness")]
    if not freshness_payloads:
        return {
            "fresh_items": 0,
            "stale_items": 0,
            "expired_items": 0,
            "oldest_data_age_minutes": None,
            "newest_data_age_minutes": None,
        }

    ages = [payload["data_age_minutes"] for payload in freshness_payloads if payload.get("data_age_minutes") is not None]
    return {
        "fresh_items": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "fresh"),
        "stale_items": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "stale"),
        "expired_items": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "expired"),
        "oldest_data_age_minutes": max(ages) if ages else None,
        "newest_data_age_minutes": min(ages) if ages else None,
    }


def _pharmacy_uses_unknown(pharmacy: str, items: list[dict]):
    for item in items:
        if not item.get("best_offer"):
            continue
        offer = next(
            (
                offer
                for offer in item.get("offers", [])
                if offer["pharmacy"] == pharmacy and offer.get("availability") != "out_of_stock"
            ),
            None,
        )
        if offer and offer.get("availability") == "unknown":
            return True
    return False


def _availability_warnings(items: list[dict]):
    warnings = []
    out_of_stock_count = sum(1 for item in items if _item_availability_state(item) == "only_out_of_stock")
    unknown_only_count = sum(1 for item in items if _item_availability_state(item) == "only_unknown")

    if out_of_stock_count:
        warnings.append(f"{out_of_stock_count} item(ns) encontrados apenas sem estoque nas farmacias monitoradas.")
    if unknown_only_count:
        warnings.append(f"{unknown_only_count} item(ns) encontrados apenas com estoque nao confirmado.")

    summary = _build_price_summary(items)
    best_basket = summary.get("best_basket_pharmacy")
    if best_basket and _pharmacy_uses_unknown(best_basket["pharmacy"], items):
        warnings.append("A melhor farmacia da cesta depende de pelo menos um item com estoque nao confirmado.")

    return warnings


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
            "offers": offers,
            "data_freshness": (_best_pricing_offer(offers) or {}).get("data_freshness"),
            "availability_summary": _item_availability_summary(
                {
                    "match_found": True,
                    "best_offer": _best_pricing_offer(offers),
                    "offers": offers,
                }
            ),
        }
        for score, canonical_product in matches
        for offers in [_canonical_offer_payload(canonical_product, latest_prices)]
    ]
    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    warnings = [] if results else ["Nenhum produto canonico encontrado para a consulta."]
    if results and confidence < 0.5:
        warnings.append("Match encontrado com baixa confianca; revisar item e ofertas.")
    if results:
        best_offer = _best_pricing_offer(results[0]["offers"])
        if not best_offer and results[0]["offers"]:
            warnings.append("Produto encontrado, mas as ofertas atuais estao sem estoque.")
        elif best_offer and best_offer.get("availability") == "unknown":
            warnings.append("Melhor oferta encontrada com estoque nao confirmado.")

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
    scores = []

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
        scores.append(score)
        offers = _canonical_offer_payload(canonical_product, latest_prices)
        comparisons.append(
            {
                "requested_item": item,
                "match_found": True,
                "quantity": 1,
                "score": score,
                "canonical_product_id": canonical_product.id,
                "canonical_name": canonical_product.canonical_name,
                "best_offer": _best_pricing_offer(offers),
                "data_freshness": (_best_pricing_offer(offers) or {}).get("data_freshness"),
                "offers": offers,
                "availability_summary": _item_availability_summary(
                    {
                        "match_found": True,
                        "best_offer": _best_pricing_offer(offers),
                        "offers": offers,
                    }
                ),
            }
        )

    unmatched_count = len([item for item in comparisons if not item["match_found"]])
    warnings = []
    if unmatched_count:
        warnings.append(f"{unmatched_count} item(ns) da lista nao tiveram match.")
    if any(item.get("match_found") and item.get("score", 0) < 50 for item in comparisons):
        warnings.append("Alguns matches da lista tem baixa confianca.")
    if comparisons and not _build_price_summary(comparisons)["best_basket_pharmacy"]:
        warnings.append("Nenhuma farmacia unica cobre toda a cesta atual.")
    warnings.extend(_availability_warnings(comparisons))

    return _tool_response(
        "compare_shopping_list",
        payload.model_dump(),
        _build_basket_result(comparisons),
        min((sum(scores) / len(scores)) / 100, 1.0) if scores else 0.0,
        warnings,
    )


@app.post("/tool/compare-basket")
def tool_compare_basket(payload: ShoppingListRequest = Body(...), db: Session = Depends(get_db)):
    result = tool_compare_shopping_list(payload, db)
    result["tool_name"] = "compare_basket"
    return result


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
        best_offer = _best_pricing_offer(offers)
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
                "data_freshness": (best_offer or {}).get("data_freshness"),
                "potential_savings": potential_savings,
                "offers": offers,
                "availability_summary": _item_availability_summary(
                    {
                        "match_found": True,
                        "best_offer": best_offer,
                        "offers": offers,
                    }
                ),
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
    warnings.extend(_availability_warnings(comparisons))

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
            **_build_basket_result(items),
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
            "offers": offers,
            "data_freshness": (_best_pricing_offer(offers) or {}).get("data_freshness"),
            "availability_summary": _item_availability_summary(
                {
                    "match_found": True,
                    "best_offer": _best_pricing_offer(offers),
                    "offers": offers,
                }
            ),
        }
        for score, canonical_product in matches
        for offers in [_canonical_offer_payload(canonical_product, latest_prices)]
    ]

    warnings = []
    if payload.source_type == "box_photo":
        warnings.append("Entrada tratada como OCR de caixa; revise lote, validade e textos promocionais ignorados.")
    if not results:
        warnings.append("Nenhum produto canonico encontrado a partir das observacoes enviadas.")
    elif results[0]["score"] < 50:
        warnings.append("Match encontrado com baixa confianca para item observado.")
    else:
        best_offer = _best_pricing_offer(results[0]["offers"])
        if not best_offer and results[0]["offers"]:
            warnings.append("Produto encontrado, mas as ofertas atuais estao sem estoque.")
        elif best_offer and best_offer.get("availability") == "unknown":
            warnings.append("Melhor oferta encontrada com estoque nao confirmado para item observado.")

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
