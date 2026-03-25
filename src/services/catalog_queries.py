import re
from datetime import UTC, datetime

from fastapi import HTTPException
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session, selectinload

from src.models.base import CanonicalProduct, CmedPriceEntry, PriceSnapshot, ProductMatch, RegulatoryAlias, RegulatoryProduct, SourceProduct
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

NON_ANCHOR_TOKENS = {
    "comprimido",
    "comprimidos",
    "capsula",
    "capsulas",
    "gota",
    "gotas",
    "solucao",
    "oral",
    "revestido",
    "revestidos",
    "uso",
    "adulto",
    "infantil",
    "seringa",
    "venda",
    "sob",
    "prescricao",
    "medica",
    "frasco",
    "caixa",
    "ampola",
}

STRUCTURAL_VARIANT_TOKENS = {
    "xr": [" xr ", " xr", "xr ", "liberacao prolongada", "prolongada", "extended release"],
    "dragea": [" dragea", " drageas"],
}


def normalize_cep(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def validate_cep_context(cep: str):
    requested_cep = normalize_cep(cep)
    if not requested_cep:
        raise HTTPException(status_code=400, detail="CEP e obrigatorio para consultar precos por regiao.")
    if len(requested_cep) != 8:
        raise HTTPException(status_code=400, detail="CEP invalido; informe um CEP com 8 digitos.")
    return requested_cep


def build_latest_price_map(db: Session, cep: str | None = None):
    normalized_cep = normalize_cep(cep) if cep else None
    try:
        latest_snapshot_query = db.query(
            PriceSnapshot.source_product_id.label("source_product_id"),
            func.max(PriceSnapshot.captured_at).label("latest_captured_at"),
        )
        if normalized_cep:
            latest_snapshot_query = latest_snapshot_query.filter(PriceSnapshot.cep == normalized_cep)
        latest_snapshot_subquery = latest_snapshot_query.group_by(PriceSnapshot.source_product_id).subquery()

        snapshots_query = db.query(PriceSnapshot).join(
            latest_snapshot_subquery,
            and_(
                PriceSnapshot.source_product_id == latest_snapshot_subquery.c.source_product_id,
                PriceSnapshot.captured_at == latest_snapshot_subquery.c.latest_captured_at,
            ),
        )
        if normalized_cep:
            snapshots_query = snapshots_query.filter(PriceSnapshot.cep == normalized_cep)
        snapshots = snapshots_query.all()
    except (AttributeError, TypeError):
        snapshots = db.query(PriceSnapshot).all()
        if normalized_cep:
            snapshots = [snapshot for snapshot in snapshots if normalize_cep(snapshot.cep) == normalized_cep]

    latest_by_source_product = {}
    for snapshot in snapshots:
        current = latest_by_source_product.get(snapshot.source_product_id)
        current_key = (
            getattr(current, "captured_at", None),
            getattr(current, "id", 0) or 0,
        )
        candidate_key = (
            getattr(snapshot, "captured_at", None),
            getattr(snapshot, "id", 0) or 0,
        )
        if current is None or candidate_key > current_key:
            latest_by_source_product[snapshot.source_product_id] = snapshot
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


def anchor_search_tokens(value: str):
    anchors = set()
    for token in tokenize_search_text(value):
        if token in NON_ANCHOR_TOKENS:
            continue
        if re.search(r"\d", token):
            continue
        if len(token) < 4:
            continue
        anchors.add(token)
    return anchors


def expand_search_queries(db: Session, query: str):
    normalized_query = normalize_query(query)
    variants = [normalized_query]
    if not normalized_query:
        return variants

    query_tokens = set(tokenize_search_text(normalized_query))
    strong_identifiers = set(re.findall(r"\b\d{8,14}\b", normalized_query))

    try:
        aliases = db.query(RegulatoryAlias).all()
        for alias in aliases:
            alias_term = normalize_query(getattr(alias, "normalized_alias", None) or getattr(alias, "alias", None))
            dcb_name = normalize_query(getattr(alias, "dcb_name", None))
            if not alias_term or not dcb_name:
                continue
            if (
                alias_term == normalized_query
                or alias_term in normalized_query
                or normalized_query in alias_term
                or alias_term in query_tokens
            ):
                variants.append(dcb_name)

        regulatory_products = db.query(RegulatoryProduct).all()
        for product in regulatory_products:
            product_name = normalize_query(getattr(product, "normalized_product_name", None) or getattr(product, "product_name", None))
            dcb_name = normalize_query(getattr(product, "dcb_name", None))
            active_ingredient = normalize_query(getattr(product, "active_ingredient", None))
            identifiers = {
                getattr(product, "ean_gtin", None),
                getattr(product, "anvisa_code", None),
            }
            matches_identifier = bool(strong_identifiers.intersection({identifier for identifier in identifiers if identifier}))
            matches_name = bool(
                product_name
                and (
                    product_name == normalized_query
                    or product_name in normalized_query
                    or normalized_query in product_name
                    or query_tokens.intersection(set(tokenize_search_text(product_name)))
                )
            )
            if matches_identifier or matches_name:
                if product_name:
                    variants.append(product_name)
                if dcb_name:
                    variants.append(dcb_name)
                if active_ingredient:
                    variants.append(active_ingredient)
    except (AttributeError, TypeError, AssertionError):
        return list(dict.fromkeys(term for term in variants if term))

    return list(dict.fromkeys(term for term in variants if term))


def preferred_search_terms(value: str):
    normalized = normalize_query(value)
    if not normalized:
        return []
    if re.fullmatch(r"\d{8,14}", normalized):
        return [normalized]

    ordered_tokens = tokenize_search_text(normalized)
    anchor_tokens = [token for token in ordered_tokens if token in anchor_search_tokens(normalized)]
    dosage_match = re.search(r"\b\d+(?:[.,]\d+)?\s*(mg|g|ui)\b", normalized)
    dosage_token = None
    if dosage_match:
        dosage_token = re.sub(r"\s+", "", dosage_match.group(0))

    terms = []
    if anchor_tokens and dosage_token:
        terms.append(f"{anchor_tokens[0]} {dosage_token}")
    if anchor_tokens:
        terms.append(anchor_tokens[0])
    if len(anchor_tokens) >= 2:
        terms.append(" ".join(anchor_tokens[:2]))
    return list(dict.fromkeys(term for term in terms if term))


def build_cmed_reference_map(db: Session):
    try:
        entries = db.query(CmedPriceEntry).all()
    except (AttributeError, TypeError, AssertionError):
        entries = []

    by_ean = {}
    by_anvisa = {}
    by_name = {}
    for entry in entries:
        if entry.ean_gtin and entry.ean_gtin not in by_ean:
            by_ean[entry.ean_gtin] = entry
        if entry.anvisa_code and entry.anvisa_code not in by_anvisa:
            by_anvisa[entry.anvisa_code] = entry
        if entry.normalized_product_name and entry.normalized_product_name not in by_name:
            by_name[entry.normalized_product_name] = entry
    return {"by_ean": by_ean, "by_anvisa": by_anvisa, "by_name": by_name}


def cmed_reference_for_product(source_product: SourceProduct, cmed_reference_map: dict | None):
    if not cmed_reference_map:
        return None
    if source_product.ean_gtin and source_product.ean_gtin in cmed_reference_map["by_ean"]:
        return cmed_reference_map["by_ean"][source_product.ean_gtin]
    if source_product.anvisa_code and source_product.anvisa_code in cmed_reference_map["by_anvisa"]:
        return cmed_reference_map["by_anvisa"][source_product.anvisa_code]

    normalized_name = normalize_query(source_product.normalized_name or source_product.raw_name)
    if normalized_name in cmed_reference_map["by_name"]:
        return cmed_reference_map["by_name"][normalized_name]
    return None


def price_validation_payload(price: float, cmed_entry: CmedPriceEntry | None):
    if not cmed_entry:
        return {
            "status": "no_reference",
            "reference_price": None,
            "source_dataset": None,
        }

    reference_price = cmed_entry.pmc_price or cmed_entry.list_price or cmed_entry.pf_price
    if not reference_price:
        return {
            "status": "reference_without_price",
            "reference_price": None,
            "source_dataset": cmed_entry.source_dataset,
        }

    if price > reference_price * 1.05:
        status = "above_reference"
    elif price < reference_price * 0.4:
        status = "below_reference"
    else:
        status = "within_reference"

    return {
        "status": status,
        "reference_price": reference_price,
        "source_dataset": cmed_entry.source_dataset,
    }


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


def structural_variant_tokens(*values: str | None):
    normalized = f" {' '.join(filter(None, [normalize_query(value) for value in values]))} "
    if not normalized.strip():
        return set()
    variants = set()
    for label, fragments in STRUCTURAL_VARIANT_TOKENS.items():
        if any(fragment in normalized for fragment in fragments):
            variants.add(label)
    return variants


def structural_variant_conflict(canonical_name: str | None, canonical_presentation: str | None, source_name: str | None, source_presentation: str | None):
    canonical_variants = structural_variant_tokens(canonical_name, canonical_presentation)
    source_variants = structural_variant_tokens(source_name, source_presentation)
    return canonical_variants != source_variants and bool(canonical_variants or source_variants)


def structural_match_payload(canonical_product: CanonicalProduct, source_product: SourceProduct):
    conflict = structural_variant_conflict(
        canonical_product.canonical_name,
        canonical_product.presentation,
        source_product.raw_name,
        source_product.presentation,
    )
    canonical_variants = sorted(structural_variant_tokens(canonical_product.canonical_name, canonical_product.presentation))
    source_variants = sorted(structural_variant_tokens(source_product.raw_name, source_product.presentation))
    return {
        "status": "conflict" if conflict else "aligned",
        "canonical_variants": canonical_variants,
        "source_variants": source_variants,
    }


def availability_rank(availability: str | None):
    if availability == "available":
        return 0
    if availability == "unknown":
        return 1
    return 2


def canonical_offer_payload(canonical_product: CanonicalProduct, latest_prices: dict, cmed_reference_map: dict | None = None):
    offers = []
    for match in canonical_product.matches:
        source_product = match.source_product
        latest_snapshot = latest_prices.get(source_product.id)
        if not latest_snapshot:
            continue
        structural_match = structural_match_payload(canonical_product, source_product)
        offers.append(
            {
                "source_product_id": source_product.id,
                "pharmacy": source_product.pharmacy.name,
                "display_name": canonical_product.canonical_name,
                "source_display_name": source_product.raw_name,
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
                "structural_match": structural_match,
                "price_validation": price_validation_payload(
                    latest_snapshot.price,
                    cmed_reference_for_product(source_product, cmed_reference_map),
                ),
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
    candidate_anchor_haystack = " ".join(
        filter(
            None,
            [
                canonical_product.normalized_name,
                source_aliases,
                canonical_product.brand,
                canonical_product.active_ingredient,
                canonical_product.manufacturer,
            ],
        )
    )
    query_anchor_tokens = anchor_search_tokens(normalized_query)
    candidate_anchor_tokens = anchor_search_tokens(candidate_anchor_haystack)
    if query_anchor_tokens and not query_anchor_tokens.intersection(candidate_anchor_tokens):
        return 0

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


def _canonical_loader_options():
    return (
        selectinload(CanonicalProduct.matches)
        .selectinload(ProductMatch.source_product)
        .selectinload(SourceProduct.pharmacy),
    )


def _candidate_tokens_for_query(query: str):
    normalized_query = normalize_query(query)
    strong_identifiers = re.findall(r"\b\d{8,14}\b", normalized_query)
    anchor_tokens = sorted(anchor_search_tokens(normalized_query))
    if anchor_tokens:
        return normalized_query, strong_identifiers, anchor_tokens[:3]

    fallback_tokens = [
        token
        for token in tokenize_search_text(normalized_query)
        if len(token) >= 3 and not re.fullmatch(r"\d+(?:[.,]\d+)?", token)
    ]
    return normalized_query, strong_identifiers, fallback_tokens[:3]


def _prefilter_canonical_ids(db: Session, query: str, limit: int):
    normalized_query, strong_identifiers, candidate_tokens = _candidate_tokens_for_query(query)
    filters = []

    if strong_identifiers:
        filters.append(or_(CanonicalProduct.ean_gtin.in_(strong_identifiers), CanonicalProduct.anvisa_code.in_(strong_identifiers)))

    for token in candidate_tokens:
        like_token = f"%{token}%"
        filters.append(
            or_(
                CanonicalProduct.normalized_name.ilike(like_token),
                CanonicalProduct.brand.ilike(like_token),
                CanonicalProduct.active_ingredient.ilike(like_token),
                CanonicalProduct.manufacturer.ilike(like_token),
                SourceProduct.normalized_name.ilike(like_token),
            )
        )

    if not filters and normalized_query:
        like_query = f"%{normalized_query}%"
        filters.append(
            or_(
                CanonicalProduct.normalized_name.ilike(like_query),
                SourceProduct.normalized_name.ilike(like_query),
            )
        )

    if not filters:
        return None

    candidate_rows = (
        db.query(CanonicalProduct.id)
        .outerjoin(CanonicalProduct.matches)
        .outerjoin(ProductMatch.source_product)
        .filter(or_(*filters))
        .distinct()
        .limit(max(limit * 20, 60))
        .all()
    )
    return [row[0] for row in candidate_rows]


def _load_candidate_canonicals(db: Session, query: str, limit: int):
    try:
        candidate_ids = set()
        for variant in expand_search_queries(db, query):
            variant_ids = _prefilter_canonical_ids(db, variant, limit)
            if variant_ids:
                candidate_ids.update(variant_ids)
        if candidate_ids:
            return (
                db.query(CanonicalProduct)
                .options(*_canonical_loader_options())
                .filter(CanonicalProduct.id.in_(list(candidate_ids)))
                .all()
            )
    except (AttributeError, TypeError, AssertionError):
        pass

    return db.query(CanonicalProduct).options(*_canonical_loader_options()).all()


def find_matching_canonicals(db: Session, query: str, limit: int = 5):
    canonical_products = _load_candidate_canonicals(db, query, limit)
    query_variants = expand_search_queries(db, query)

    ranked = []
    for canonical_product in canonical_products:
        score = max(score_canonical_match(canonical_product, variant) for variant in query_variants)
        if score >= 35:
            ranked.append((score, canonical_product))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return ranked[:limit]


def _source_product_match_score(source_product: SourceProduct, query_variants: list[str]):
    source_haystacks = [
        normalize_query(source_product.raw_name),
        normalize_query(source_product.normalized_name),
        normalize_query(source_product.active_ingredient),
        normalize_query(source_product.brand),
    ]
    source_haystacks = [value for value in source_haystacks if value]
    if not source_haystacks:
        return 0

    best_score = 0
    source_tokens = set()
    for haystack in source_haystacks:
        source_tokens.update(tokenize_search_text(haystack))

    for variant in query_variants:
        normalized_variant = normalize_query(variant)
        if not normalized_variant:
            continue
        variant_score = 0
        if any(haystack == normalized_variant for haystack in source_haystacks):
            variant_score += 80
        if any(normalized_variant in haystack or haystack in normalized_variant for haystack in source_haystacks):
            variant_score += 20

        variant_tokens = set(tokenize_search_text(normalized_variant))
        overlap = variant_tokens.intersection(source_tokens)
        if overlap:
            variant_score += min(len(overlap) * 15, 45)
        if variant_tokens and overlap == variant_tokens:
            variant_score += 25
        best_score = max(best_score, variant_score)

    return best_score


def find_matching_canonicals_from_source_products(db: Session, query: str, latest_prices: dict, limit: int = 5):
    query_variants = expand_search_queries(db, query)
    source_products = db.query(SourceProduct).options(selectinload(SourceProduct.pharmacy), selectinload(SourceProduct.match)).all()

    ranked = {}
    for source_product in source_products:
        if source_product.id not in latest_prices:
            continue
        if not source_product.match or not source_product.match.canonical_product_id:
            continue
        match = source_product.match
        canonical_product = getattr(match, "canonical_product", None)
        if not canonical_product:
            continue

        source_score = _source_product_match_score(source_product, query_variants)
        canonical_score = max(score_canonical_match(canonical_product, variant) for variant in query_variants)
        score = max(source_score, canonical_score)
        if score < 35:
            continue

        current = ranked.get(canonical_product.id)
        if not current or score > current[0]:
            ranked[canonical_product.id] = (score, canonical_product)

    ordered = sorted(ranked.values(), key=lambda item: item[0], reverse=True)
    return ordered[:limit]
