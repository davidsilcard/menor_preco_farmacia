import re
from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy.orm import Session, joinedload

from src.core.config import settings
from src.models.base import CanonicalProduct, PriceSnapshot, ProductMatch, SourceProduct
from src.scrapers.base import BaseScraper

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


def normalize_cep(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def validate_cep_context(cep: str):
    requested_cep = normalize_cep(cep)
    active_cep = normalize_cep(settings.CEP)
    if not requested_cep:
        raise HTTPException(status_code=400, detail="CEP e obrigatorio para consultar preços por regiao.")
    if requested_cep != active_cep:
        raise HTTPException(
            status_code=409,
            detail=f"Os dados atuais foram coletados para o CEP {settings.CEP}. Recolete os scrapers para o CEP solicitado.",
        )
    return requested_cep


def build_latest_price_map(db: Session):
    snapshots = (
        db.query(PriceSnapshot)
        .order_by(PriceSnapshot.source_product_id.asc(), PriceSnapshot.captured_at.desc())
        .all()
    )
    latest_by_source_product = {}
    for snapshot in snapshots:
        latest_by_source_product.setdefault(snapshot.source_product_id, snapshot)
    return latest_by_source_product


def data_age_minutes(captured_at):
    if not captured_at:
        return None
    now_utc = datetime.now(UTC).replace(tzinfo=None)
    return max(int((now_utc - captured_at).total_seconds() // 60), 0)


def freshness_status(captured_at):
    age_minutes = data_age_minutes(captured_at)
    if age_minutes is None:
        return "unknown"
    if age_minutes <= FRESH_DATA_MAX_AGE_MINUTES:
        return "fresh"
    if age_minutes <= STALE_DATA_MAX_AGE_MINUTES:
        return "stale"
    return "expired"


def snapshot_freshness_payload(snapshot: PriceSnapshot):
    return {
        "captured_at": snapshot.captured_at,
        "data_age_minutes": data_age_minutes(snapshot.captured_at),
        "freshness_status": freshness_status(snapshot.captured_at),
        "scrape_run_id": snapshot.scrape_run_id,
    }


def normalize_query(value: str) -> str:
    normalized = BaseScraper.normalize_text(value or "")
    normalized = re.sub(r"r\$\s*\d+[.,]?\d*", " ", normalized)
    normalized = re.sub(r"\b\d{4,}\b", lambda match: match.group(0), normalized)
    normalized = re.sub(r"[^a-z0-9\s/.,-]", " ", normalized)

    for alias, expanded in SEARCH_TERM_ALIASES.items():
        normalized = re.sub(rf"\b{re.escape(alias)}\b", expanded, normalized)

    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def tokenize_search_text(value: str):
    normalized = normalize_query(value)
    tokens = []
    for token in normalized.split():
        if token in SEARCH_STOPWORDS:
            continue
        if len(token) == 1 and not token.isdigit():
            continue
        tokens.append(token)
    return tokens


def significant_search_tokens(value: str):
    tokens = tokenize_search_text(value)
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


def has_special_token_conflict(query: str, candidate: str):
    normalized_query = normalize_query(query)
    if re.fullmatch(r"\d{8,14}", normalized_query):
        return False

    query_tokens = significant_search_tokens(query)
    candidate_tokens = significant_search_tokens(candidate)
    query_special = {token for token in SEARCH_SPECIAL_TOKENS if token in query_tokens}
    candidate_special = {token for token in SEARCH_SPECIAL_TOKENS if token in candidate_tokens}

    if any(token not in candidate_special for token in query_special):
        return True

    if any(token in candidate_special and token not in query_special for token in STRICT_CANDIDATE_SPECIAL_TOKENS):
        return True

    return False


def availability_rank(availability: str | None):
    if availability == "available":
        return 0
    if availability == "unknown":
        return 1
    return 2


def canonical_offer_payload(canonical_product: CanonicalProduct, latest_prices: dict):
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
                "data_freshness": snapshot_freshness_payload(latest_snapshot),
                "ean_gtin": source_product.ean_gtin,
                "anvisa_code": source_product.anvisa_code,
                "match_type": match.match_type,
                "match_confidence": match.confidence,
                "review_status": match.review_status,
                "review_notes": match.review_notes,
            }
        )

    offers.sort(key=lambda offer: (availability_rank(offer.get("availability")), offer["price"]))
    return offers


def pricing_eligible_offers(offers: list[dict]):
    return [offer for offer in offers if offer.get("availability") != "out_of_stock"]


def best_pricing_offer(offers: list[dict]):
    eligible = pricing_eligible_offers(offers)
    eligible.sort(key=lambda offer: (availability_rank(offer.get("availability")), offer["price"]))
    return eligible[0] if eligible else None


def score_canonical_match(canonical_product: CanonicalProduct, query: str):
    normalized_query = normalize_query(query)
    tokens = tokenize_search_text(query)
    if has_special_token_conflict(normalized_query, canonical_product.normalized_name):
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

    query_tokens = significant_search_tokens(normalized_query)
    candidate_tokens = significant_search_tokens(canonical_product.normalized_name)
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

    normalized_dosage = normalize_query(canonical_product.dosage) if canonical_product.dosage else None
    if normalized_dosage and normalized_dosage in normalized_query:
        score += 20
    elif normalized_dosage:
        dosage_prefix = normalized_dosage.split("/")[0]
        if dosage_prefix and dosage_prefix in normalized_query:
            score += 18
    if canonical_product.pack_size and normalize_query(canonical_product.pack_size) in normalized_query:
        score += 20
    if canonical_product.brand and normalize_query(canonical_product.brand) in normalized_query:
        score += 15

    strong_digits = re.findall(r"\b\d{8,14}\b", normalized_query)
    for digits in strong_digits:
        if canonical_product.ean_gtin == digits:
            score += 100
        if canonical_product.anvisa_code == digits:
            score += 95

    return score


def find_matching_canonicals(db: Session, query: str, limit: int = 5):
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
        score = score_canonical_match(canonical_product, query)
        if score > 0:
            ranked.append((score, canonical_product))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[:limit]
