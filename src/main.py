import re
from datetime import UTC, datetime

from fastapi import Body, Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session, joinedload

from src.core.config import settings
from src.models.base import (
    CatalogRequest,
    CanonicalProduct,
    Pharmacy,
    PriceSnapshot,
    ProductMatch,
    ScrapeRun,
    SearchJob,
    SessionLocal,
    SourceProduct,
    TrackedItemByCep,
)
from src.scrapers.base import BaseScraper
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

FRESH_DATA_MAX_AGE_MINUTES = 12 * 60
STALE_DATA_MAX_AGE_MINUTES = 24 * 60
SEARCH_JOB_ETA_SECONDS = 15 * 60
TRACKED_ITEM_ACTIVE_DAYS = 30
TRACKED_ITEM_INACTIVE_DAYS = 90

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
        "operational_endpoints": [
            "/catalog/requests",
            "/search-jobs",
            "/search-jobs/{job_id}",
            "/tracked-items",
            "/ops/collection-plan",
            "/ops/collections/run",
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


def _register_catalog_request(db: Session, query: str, cep: str, tool_name: str):
    normalized_query = _normalize_query(query)
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


def _catalog_request_payload(request: CatalogRequest | None):
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


def _tracked_item_status(last_requested_at):
    age_minutes = _data_age_minutes(last_requested_at)
    if age_minutes is None:
        return "active"
    age_days = age_minutes / (60 * 24)
    if age_days > TRACKED_ITEM_INACTIVE_DAYS:
        return "inactive"
    if age_days > TRACKED_ITEM_ACTIVE_DAYS:
        return "cooldown"
    return "active"


def _tracked_item_priority(request_count_total: int, last_requested_at, canonical_product_id: int | None):
    age_minutes = _data_age_minutes(last_requested_at) or 0
    age_days = age_minutes / (60 * 24)
    status = _tracked_item_status(last_requested_at)
    if status == "inactive":
        return 0.0

    base = 100.0 if status == "active" else 40.0
    demand_bonus = min(request_count_total, 25) * 2.0
    recency_penalty = min(age_days, 30) * 1.5
    canonical_bonus = 10.0 if canonical_product_id else 0.0
    return round(max(base + demand_bonus + canonical_bonus - recency_penalty, 0.0), 2)


def _tracked_item_payload(item: TrackedItemByCep | None):
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


def _register_tracked_item(
    db: Session,
    query: str,
    cep: str,
    tool_name: str,
    *,
    canonical_product: CanonicalProduct | None = None,
    source_kind: str | None = None,
    match_confidence: float | None = None,
):
    normalized_query = _normalize_query(query)
    if not normalized_query:
        return None

    tracked_item = (
        db.query(TrackedItemByCep)
        .filter(TrackedItemByCep.cep == cep, TrackedItemByCep.normalized_query == normalized_query)
        .first()
    )

    if not tracked_item and canonical_product:
        tracked_item = (
            db.query(TrackedItemByCep)
            .filter(TrackedItemByCep.cep == cep, TrackedItemByCep.canonical_product_id == canonical_product.id)
            .first()
        )

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

    tracked_item.status = _tracked_item_status(tracked_item.last_requested_at)
    tracked_item.scrape_priority = _tracked_item_priority(
        tracked_item.request_count_total,
        tracked_item.last_requested_at,
        tracked_item.canonical_product_id,
    )
    db.commit()
    db.refresh(tracked_item)
    return tracked_item


def _queued_job_position(db: Session, current_job_id: int | None = None):
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


def _search_job_payload(job: SearchJob | None, db: Session | None = None):
    if not job:
        return None

    position = job.position_hint
    if db and job.status in {"queued", "processing"}:
        position = _queued_job_position(db, job.id)

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


def _queue_metrics(db: Session):
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
            _data_age_minutes(min(job.created_at for job in queued))
            if queued
            else None
        ),
    }


def _scrape_run_payload(run: ScrapeRun):
    duration_seconds = None
    if run.finished_at:
        duration_seconds = max(int((run.finished_at - run.started_at).total_seconds()), 0)
    return {
        "scrape_run_id": run.id,
        "pharmacy": run.pharmacy.name if run.pharmacy else None,
        "cep": run.cep,
        "trigger_type": run.trigger_type,
        "status": run.status,
        "search_terms": run.search_terms or [],
        "products_seen": run.products_seen,
        "products_saved": run.products_saved,
        "error_count": run.error_count,
        "error_message": run.error_message,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "duration_seconds": duration_seconds,
    }


def _pharmacy_metrics(db: Session):
    source_products = (
        db.query(SourceProduct)
        .options(
            joinedload(SourceProduct.pharmacy),
            joinedload(SourceProduct.match),
        )
        .all()
    )
    latest_prices = _build_latest_price_map(db)
    metrics = {}

    for product in source_products:
        pharmacy_name = product.pharmacy.name
        bucket = metrics.setdefault(
            pharmacy_name,
            {
                "source_products": 0,
                "matched_products": 0,
                "auto_approved_matches": 0,
                "needs_review_matches": 0,
                "availability_counts": {"available": 0, "unknown": 0, "out_of_stock": 0},
                "latest_snapshot_age_minutes": None,
            },
        )
        bucket["source_products"] += 1
        if product.match:
            bucket["matched_products"] += 1
            if product.match.review_status == "auto_approved":
                bucket["auto_approved_matches"] += 1
            if product.match.review_status == "needs_review":
                bucket["needs_review_matches"] += 1

        latest_snapshot = latest_prices.get(product.id)
        if latest_snapshot:
            availability = latest_snapshot.availability or "unknown"
            bucket["availability_counts"][availability] = bucket["availability_counts"].get(availability, 0) + 1
            age = _data_age_minutes(latest_snapshot.captured_at)
            current_oldest = bucket["latest_snapshot_age_minutes"]
            bucket["latest_snapshot_age_minutes"] = age if current_oldest is None else max(current_oldest, age)

    for bucket in metrics.values():
        bucket["match_rate"] = round(bucket["matched_products"] / bucket["source_products"], 4) if bucket["source_products"] else 0.0
        bucket["auto_approved_rate"] = (
            round(bucket["auto_approved_matches"] / bucket["matched_products"], 4) if bucket["matched_products"] else 0.0
        )

    return metrics


def _ops_health_payload(db: Session):
    queue = _queue_metrics(db)
    last_runs = (
        db.query(ScrapeRun)
        .options(joinedload(ScrapeRun.pharmacy))
        .order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc())
        .limit(20)
        .all()
    )
    last_run_by_pharmacy = {}
    for run in last_runs:
        pharmacy_name = run.pharmacy.name if run.pharmacy else f"pharmacy:{run.pharmacy_id}"
        last_run_by_pharmacy.setdefault(pharmacy_name, run)

    stale_pharmacies = []
    failed_pharmacies = []
    for pharmacy_name, run in last_run_by_pharmacy.items():
        if run.status == "failed":
            failed_pharmacies.append(pharmacy_name)
        elif _freshness_status(run.started_at) != "fresh":
            stale_pharmacies.append(pharmacy_name)

    overall_status = "healthy"
    if failed_pharmacies:
        overall_status = "degraded"
    elif queue["queued_jobs"] > 20 or stale_pharmacies:
        overall_status = "attention"

    return {
        "status": overall_status,
        "active_cep": settings.CEP,
        "queue": queue,
        "stale_pharmacies": sorted(stale_pharmacies),
        "failed_pharmacies": sorted(failed_pharmacies),
        "last_scrape_runs": [_scrape_run_payload(run) for run in last_run_by_pharmacy.values()],
    }


def _register_search_job(
    db: Session,
    query: str,
    cep: str,
    tool_name: str,
    catalog_request: CatalogRequest | None = None,
):
    normalized_query = _normalize_query(query)
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
        existing.position_hint = _queued_job_position(db, existing.id)
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
    job.position_hint = _queued_job_position(db, job.id)
    job.eta_seconds = max(job.position_hint - 1, 0) * SEARCH_JOB_ETA_SECONDS
    db.commit()
    db.refresh(job)
    return job


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
    try:
        return compare_canonical_product_service(canonical_product_id, db)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/matching/review")
def list_pending_reviews(db: Session = Depends(get_db)):
    return list_review_matches_service(db)


@app.get("/catalog/requests")
def list_catalog_requests(db: Session = Depends(get_db)):
    requests = db.query(CatalogRequest).order_by(CatalogRequest.last_requested_at.desc()).all()
    return [_catalog_request_payload(request) for request in requests]


@app.get("/search-jobs")
def list_search_jobs(db: Session = Depends(get_db)):
    jobs = db.query(SearchJob).order_by(SearchJob.created_at.desc(), SearchJob.id.desc()).all()
    return [_search_job_payload(job, db) for job in jobs]


@app.get("/search-jobs/{job_id}")
def get_search_job(job_id: int, db: Session = Depends(get_db)):
    job = db.query(SearchJob).filter(SearchJob.id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Search job nao encontrado")
    return _search_job_payload(job, db)


@app.get("/tracked-items")
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
        query = query.filter(TrackedItemByCep.cep == _normalize_cep(cep))
    if status:
        query = query.filter(TrackedItemByCep.status == status)
    elif not include_inactive:
        query = query.filter(TrackedItemByCep.status != "inactive")

    items = query.all()
    return [_tracked_item_payload(item) for item in items]


@app.get("/ops/collection-plan")
def get_collection_plan(cep: str | None = Query(None)):
    from src.services.scheduled_collection import build_scheduled_collection_plan

    normalized_cep = _normalize_cep(cep) if cep else None
    return build_scheduled_collection_plan(normalized_cep)


@app.get("/ops/health")
def ops_health(db: Session = Depends(get_db)):
    return _ops_health_payload(db)


@app.get("/ops/scrape-runs")
def list_scrape_runs(limit: int = Query(20, ge=1, le=200), db: Session = Depends(get_db)):
    runs = (
        db.query(ScrapeRun)
        .options(joinedload(ScrapeRun.pharmacy))
        .order_by(ScrapeRun.started_at.desc(), ScrapeRun.id.desc())
        .limit(limit)
        .all()
    )
    return [_scrape_run_payload(run) for run in runs]


@app.get("/ops/metrics")
def ops_metrics(db: Session = Depends(get_db)):
    latest_prices = _build_latest_price_map(db)
    matches = db.query(ProductMatch).all()
    latest_snapshots = list(latest_prices.values())
    availability_counts = {"available": 0, "unknown": 0, "out_of_stock": 0}
    for snapshot in latest_snapshots:
        availability = snapshot.availability or "unknown"
        availability_counts[availability] = availability_counts.get(availability, 0) + 1

    match_type_counts = {}
    review_status_counts = {}
    for match in matches:
        match_type_counts[match.match_type] = match_type_counts.get(match.match_type, 0) + 1
        review_status_counts[match.review_status] = review_status_counts.get(match.review_status, 0) + 1

    return {
        "active_cep": settings.CEP,
        "catalog": {
            "canonical_products": db.query(CanonicalProduct).count(),
            "source_products": db.query(SourceProduct).count(),
            "latest_snapshots": len(latest_snapshots),
        },
        "matching": {
            "total_matches": len(matches),
            "match_type_counts": match_type_counts,
            "review_status_counts": review_status_counts,
        },
        "availability": availability_counts,
        "queue": _queue_metrics(db),
        "catalog_requests": {
            "pending": db.query(CatalogRequest).filter(CatalogRequest.status == "pending").count(),
            "total": db.query(CatalogRequest).count(),
        },
        "tracked_items": {
            "total": db.query(TrackedItemByCep).count(),
            "active": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "active").count(),
            "cooldown": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "cooldown").count(),
            "inactive": db.query(TrackedItemByCep).filter(TrackedItemByCep.status == "inactive").count(),
        },
        "pharmacies": _pharmacy_metrics(db),
    }


@app.post("/ops/search-jobs/process-next")
def process_next_search_job_endpoint():
    from src.services.search_jobs import process_next_search_job

    job = process_next_search_job()
    if not job:
        return {"message": "Nenhum search job pendente na fila."}
    with SessionLocal() as db:
        refreshed = db.query(SearchJob).filter(SearchJob.id == job.id).first()
        return _search_job_payload(refreshed, db)


@app.post("/ops/collections/run")
def run_scheduled_collection_endpoint(cep: str | None = Query(None)):
    from src.services.scheduled_collection import run_scheduled_collection

    normalized_cep = _normalize_cep(cep) if cep else None
    return run_scheduled_collection(normalized_cep)


@app.post("/ops/search-jobs/{job_id}/process")
def process_search_job_endpoint(job_id: int):
    from src.services.search_jobs import process_search_job

    job = process_search_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Search job nao encontrado")
    with SessionLocal() as db:
        refreshed = db.query(SearchJob).filter(SearchJob.id == job.id).first()
        return _search_job_payload(refreshed, db)


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
