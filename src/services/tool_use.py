import re
from datetime import UTC, datetime
from urllib.parse import urlparse

from fastapi import HTTPException
from sqlalchemy.orm import Session, joinedload

from src.models.base import CanonicalProduct, PharmacyLead, ProductMatch, SourceProduct
from src.services.catalog_queries import (
    availability_rank,
    best_pricing_offer,
    build_latest_price_map,
    build_cmed_reference_map,
    canonical_offer_payload,
    find_matching_canonicals,
    find_matching_canonicals_from_source_products,
    normalize_query,
    validate_cep_context,
)
from src.services.demand_tracking import (
    catalog_request_payload,
    register_catalog_request,
    register_search_job,
    register_tracked_item,
    search_job_payload,
    tracked_item_payload,
)
from src.services.operation_jobs import (
    JOB_TYPE_PROCESS_SEARCH_JOB,
    enqueue_operation_job,
    operation_job_payload,
)
from src.services.tool_models import (
    InvoiceComparisonRequest,
    ObservedItemRequest,
    PharmacyLeadRequest,
    ReceiptComparisonRequest,
    ShoppingListRequest,
)


def tool_response(tool_name: str, tool_input: dict, result, confidence: float, warnings: list[str] | None = None):
    return {
        "tool_name": tool_name,
        "input": tool_input,
        "confidence": round(confidence, 2),
        "warnings": warnings or [],
        "result": result,
    }


def _enqueue_search_job_processing(db: Session, *, search_job, requested_by: str):
    if not search_job:
        return None
    operation_job = enqueue_operation_job(
        db,
        job_type=JOB_TYPE_PROCESS_SEARCH_JOB,
        requested_by=requested_by,
        payload={"search_job_id": search_job.id, "cep": search_job.cep},
    )
    return operation_job_payload(operation_job)


def estimate_overall_confidence(items: list[dict]):
    scores = [item.get("score", 0) for item in items if item.get("match_found")]
    if not scores:
        return 0.0
    return min((sum(scores) / len(scores)) / 100, 1.0)


def resolution_source_summary(items: list[dict]):
    counts = {}
    for item in items:
        source = item.get("resolution_source")
        if not source:
            continue
        counts[source] = counts.get(source, 0) + 1
    return counts


def results_offer_count(results: list[dict]):
    return sum(len(result.get("offers") or []) for result in results)


def unique_pharmacies(results: list[dict]):
    pharmacies = {
        offer["pharmacy"]
        for result in results
        for offer in (result.get("offers") or [])
        if offer.get("pharmacy")
    }
    return sorted(pharmacies)


def results_with_offers_count(results: list[dict]):
    return sum(1 for result in results if result.get("offers"))


def structural_conflict_count(results: list[dict]):
    return sum(
        1
        for result in results
        for offer in (result.get("offers") or [])
        if (offer.get("structural_match") or {}).get("status") == "conflict"
    )


def canonical_display_name(canonical_product: CanonicalProduct):
    parts = [canonical_product.canonical_name]
    normalized_name = normalize_query(canonical_product.canonical_name)
    for extra in [canonical_product.dosage, canonical_product.presentation, canonical_product.pack_size]:
        normalized_extra = normalize_query(extra) if extra else None
        if extra and normalized_extra and normalized_extra not in normalized_name:
            parts.append(extra)
    return " - ".join(parts)


def canonical_presentation_group(canonical_product: CanonicalProduct):
    parts = [part for part in [canonical_product.dosage, canonical_product.presentation, canonical_product.pack_size] if part]
    if parts:
        return " | ".join(parts)
    return canonical_product.canonical_name


def recommended_match_mode(query: str):
    return "strict" if requested_strength_tokens(query) else "broad"


def results_freshness_summary(results: list[dict]):
    best_offers = [best_pricing_offer(result.get("offers") or []) for result in results]
    freshness_payloads = [offer.get("data_freshness") for offer in best_offers if offer and offer.get("data_freshness")]
    counts = {
        "fresh_results": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "fresh"),
        "stale_results": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "stale"),
        "expired_results": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "expired"),
        "unknown_results": sum(1 for payload in freshness_payloads if payload.get("freshness_status") == "unknown"),
    }
    if not freshness_payloads:
        quality = "unknown"
    elif counts["expired_results"] == len(freshness_payloads):
        quality = "expired_only"
    elif counts["fresh_results"] > 0:
        quality = "fresh_available"
    elif counts["stale_results"] > 0:
        quality = "stale_only"
    else:
        quality = "unknown"
    return {
        **counts,
        "results_with_freshness": len(freshness_payloads),
        "quality": quality,
    }


def next_action_for_results(results: list[dict], *, requires_polling: bool):
    if requires_polling:
        return "poll_search_job", "A busca foi enfileirada e ainda depende do resultado do search_job."
    if results:
        freshness_summary = results_freshness_summary(results)
        if freshness_summary["quality"] == "expired_only":
            return "respond_with_caution", "Existem ofertas, mas os melhores snapshots estao expirados e podem nao refletir o preco atual."
        return "respond_now", "Ja existem resultados estruturados suficientes para responder ao usuario."
    return "ask_user_to_refine", "Nao ha resultado util imediato; vale pedir mais detalhes da apresentacao ou dosagem."


def grouped_results(results: list[dict]):
    grouped = {}
    for result in results:
        group_key = result.get("presentation_group") or result.get("display_name") or result.get("canonical_name")
        group = grouped.setdefault(
            group_key,
            {
                "group_label": group_key,
                "items": [],
                "results_count": 0,
                "offers_count": 0,
                "unique_pharmacies": set(),
                "best_offer": None,
                "structural_conflict_count": 0,
            },
        )
        group["items"].append(result)
        group["results_count"] += 1
        group["offers_count"] += len(result.get("offers") or [])
        group["structural_conflict_count"] += sum(
            1 for offer in (result.get("offers") or []) if (offer.get("structural_match") or {}).get("status") == "conflict"
        )
        for offer in result.get("offers") or []:
            if offer.get("pharmacy"):
                group["unique_pharmacies"].add(offer["pharmacy"])
        candidate_best_offer = best_pricing_offer(result.get("offers") or [])
        current_best_offer = group["best_offer"]
        if candidate_best_offer and (
            current_best_offer is None
            or (candidate_best_offer["price"], availability_rank(candidate_best_offer.get("availability")))
            < (current_best_offer["price"], availability_rank(current_best_offer.get("availability")))
        ):
            group["best_offer"] = candidate_best_offer

    payload = []
    for group in grouped.values():
        payload.append(
            {
                "group_label": group["group_label"],
                "results_count": group["results_count"],
                "offers_count": group["offers_count"],
                "unique_pharmacies_count": len(group["unique_pharmacies"]),
                "unique_pharmacies": sorted(group["unique_pharmacies"]),
                "structural_conflict_count": group["structural_conflict_count"],
                "best_offer": group["best_offer"],
                "items": group["items"],
            }
        )
    payload.sort(key=lambda item: item["group_label"])
    return payload


def evidence_level(resolution_source: str | None, results: list[dict]):
    if results_offer_count(results) > 0:
        return "real_offer"
    if resolution_source == "source_product_fallback" and results:
        return "source_product"
    if results:
        return "canonical_only"
    return "none"


def outcome_for_results(results: list[dict], *, requires_polling: bool):
    if requires_polling and not results:
        return "queued"
    if requires_polling and results:
        return "partial"
    if results:
        return "resolved"
    return "no_results"


def flattened_tool_refs(*, catalog_request=None, search_job=None, operation_job=None, tracked_item=None):
    return {
        "catalog_request_id": catalog_request.get("catalog_request_id") if catalog_request else None,
        "search_job_id": search_job.get("job_id") if search_job else None,
        "operation_job_id": operation_job.get("operation_job_id") if operation_job else None,
        "tracked_item_id": tracked_item.get("tracked_item_id") if tracked_item else None,
    }


def flattened_tool_ref_lists(*, catalog_requests=None, search_jobs=None, operation_jobs=None, tracked_items=None):
    return {
        "catalog_request_ids": [item["catalog_request_id"] for item in (catalog_requests or []) if item.get("catalog_request_id") is not None],
        "search_job_ids": [item["job_id"] for item in (search_jobs or []) if item.get("job_id") is not None],
        "operation_job_ids": [item["operation_job_id"] for item in (operation_jobs or []) if item and item.get("operation_job_id") is not None],
        "tracked_item_ids": [item["tracked_item_id"] for item in (tracked_items or []) if item.get("tracked_item_id") is not None],
    }


def normalize_website_url(url: str):
    value = (url or "").strip()
    if not value:
        raise HTTPException(status_code=400, detail="website_url e obrigatoria.")
    if "://" not in value:
        value = f"https://{value}"
    parsed = urlparse(value)
    domain = (parsed.netloc or parsed.path or "").strip().lower()
    domain = domain.removeprefix("www.")
    if not domain or "." not in domain:
        raise HTTPException(status_code=400, detail="website_url invalida para sugestao de farmacia.")
    normalized_url = f"{parsed.scheme or 'https'}://{domain}{parsed.path if parsed.netloc else ''}".rstrip("/")
    return normalized_url, domain


def normalize_match_mode(match_mode: str | None):
    normalized = (match_mode or "broad").strip().lower()
    if normalized not in {"broad", "strict"}:
        raise HTTPException(status_code=400, detail="match_mode invalido; use 'broad' ou 'strict'.")
    return normalized


def pharmacy_lead_payload(lead: PharmacyLead):
    return {
        "pharmacy_lead_id": lead.id,
        "pharmacy_name": lead.pharmacy_name,
        "website_url": lead.website_url,
        "normalized_domain": lead.normalized_domain,
        "suggested_cep": lead.suggested_cep,
        "suggested_city": lead.suggested_city,
        "suggested_state": lead.suggested_state,
        "notes": lead.notes,
        "status": lead.status,
        "suggestion_count": lead.suggestion_count,
        "first_suggested_at": lead.first_suggested_at,
        "last_suggested_at": lead.last_suggested_at,
        "last_suggested_by_tool": lead.last_suggested_by_tool,
    }


def requested_strength_tokens(query: str):
    normalized_query = normalize_query(query)
    matches = re.findall(r"\b\d+(?:[.,]\d+)?\s*(?:mg/ml|mg|g|ml|ui)\b", normalized_query)
    return [re.sub(r"\s+", "", match) for match in matches]


def result_strength_tokens(result: dict):
    haystacks = [
        result.get("display_name"),
        result.get("presentation_group"),
        result.get("canonical_name"),
    ]
    tokens = set()
    for value in haystacks:
        normalized = normalize_query(value or "")
        matches = re.findall(r"\b\d+(?:[.,]\d+)?\s*(?:mg/ml|mg|g|ml|ui)\b", normalized)
        tokens.update(re.sub(r"\s+", "", match) for match in matches)
    return tokens


def filter_results_by_match_mode(results: list[dict], query: str, match_mode: str):
    if match_mode != "strict":
        return results

    strengths = requested_strength_tokens(query)
    if not strengths:
        return results

    filtered = []
    for result in results:
        candidate_strengths = result_strength_tokens(result)
        if all(strength in candidate_strengths for strength in strengths):
            filtered.append(result)
    return filtered


def find_matches_with_resolution_source(db: Session, query: str, latest_prices: dict, *, limit: int | None = None):
    matches = find_matching_canonicals(db, query, limit=limit) if limit is not None else find_matching_canonicals(db, query)
    if matches:
        return matches, "canonical_match"

    fallback_matches = find_matching_canonicals_from_source_products(db, query, latest_prices, limit=limit or 5)
    if fallback_matches:
        return fallback_matches, "source_product_fallback"

    return [], None


def build_observed_query(payload: ObservedItemRequest):
    observations = []
    for value in payload.observations:
        normalized = normalize_query(value)
        if not normalized:
            continue
        if re.search(r"\b(lote|validade|fab|fabricacao)\b", normalized):
            continue
        observations.append(normalized)
    return " ".join(dict.fromkeys(observations)).strip()


def item_availability_state(item: dict):
    offers = item.get("offers", [])
    eligible = [offer for offer in offers if offer.get("availability") != "out_of_stock"]
    if offers and not eligible:
        return "only_out_of_stock"
    if eligible and all(offer.get("availability") == "unknown" for offer in eligible):
        return "only_unknown"
    return None


def item_availability_summary(item: dict):
    offers = item.get("offers", [])
    counts = {"available": 0, "unknown": 0, "out_of_stock": 0}
    for offer in offers:
        availability = offer.get("availability") or "unknown"
        counts[availability] = counts.get(availability, 0) + 1

    state = item_availability_state(item)
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


def basket_availability_summary(items: list[dict]):
    item_summaries = [item_availability_summary(item) for item in items if item.get("match_found")]
    return {
        "items_with_available_offers": sum(1 for summary in item_summaries if summary["state"] == "has_available_offers"),
        "items_only_unknown_offers": sum(1 for summary in item_summaries if summary["state"] == "only_unknown_offers"),
        "items_only_out_of_stock_offers": sum(1 for summary in item_summaries if summary["state"] == "only_out_of_stock_offers"),
        "items_without_offers": sum(1 for summary in item_summaries if summary["state"] == "no_offers"),
    }


def basket_freshness_summary(items: list[dict]):
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


def build_price_summary(items: list[dict]):
    total_paid = round(sum((item.get("paid_price") or 0) * (item.get("quantity") or 1) for item in items), 2)
    matched_items = [item for item in items if item.get("match_found")]
    priced_items = [item for item in matched_items if item.get("best_offer")]
    total_best_price = round(
        sum((item.get("best_offer", {}).get("price") or 0) * (item.get("quantity") or 1) for item in priced_items if item.get("best_offer")),
        2,
    )
    total_potential_savings = round(sum(item.get("potential_savings") or 0 for item in items), 2)

    candidate_pharmacies = sorted({offer["pharmacy"] for item in matched_items for offer in item.get("offers", [])})
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


def build_basket_result(items: list[dict]):
    summary = build_price_summary(items)
    return {
        "items": items,
        "summary": summary,
        "availability_summary": basket_availability_summary(items),
        "data_freshness": basket_freshness_summary(items),
    }


def pharmacy_uses_unknown(pharmacy: str, items: list[dict]):
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


def availability_warnings(items: list[dict]):
    warnings = []
    out_of_stock_count = sum(1 for item in items if item_availability_state(item) == "only_out_of_stock")
    unknown_only_count = sum(1 for item in items if item_availability_state(item) == "only_unknown")
    if out_of_stock_count:
        warnings.append(f"{out_of_stock_count} item(ns) encontrados apenas sem estoque nas farmacias monitoradas.")
    if unknown_only_count:
        warnings.append(f"{unknown_only_count} item(ns) encontrados apenas com estoque nao confirmado.")

    summary = build_price_summary(items)
    best_basket = summary.get("best_basket_pharmacy")
    if best_basket and pharmacy_uses_unknown(best_basket["pharmacy"], items):
        warnings.append("A melhor farmacia da cesta depende de pelo menos um item com estoque nao confirmado.")
    return warnings


def search_products_service(query: str, cep: str, db: Session, *, match_mode: str = "broad"):
    normalized_match_mode = normalize_match_mode(match_mode)
    recommended_mode = recommended_match_mode(query)
    requested_cep = validate_cep_context(cep)
    latest_prices = build_latest_price_map(db, requested_cep)
    cmed_reference_map = build_cmed_reference_map(db)
    matches, resolution_source = find_matches_with_resolution_source(db, query, latest_prices)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "display_name": canonical_display_name(canonical_product),
            "presentation_group": canonical_presentation_group(canonical_product),
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": offers,
            "data_freshness": (best_pricing_offer(offers) or {}).get("data_freshness"),
            "availability_summary": item_availability_summary(
                {"match_found": True, "best_offer": best_pricing_offer(offers), "offers": offers}
            ),
        }
        for score, canonical_product in matches
        for offers in [canonical_offer_payload(canonical_product, latest_prices, cmed_reference_map)]
    ]
    unfiltered_results_count = len(results)
    results = filter_results_by_match_mode(results, query, normalized_match_mode)
    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    catalog_request = None
    search_job = None
    operation_job = None
    tracked_item = None
    warnings = [] if results else ["Nenhum produto canonico encontrado para a consulta."]
    if normalized_match_mode == "strict" and unfiltered_results_count > len(results):
        warnings.append("Modo estrito removeu variacoes com dosagem diferente da solicitada.")
    if results and confidence < 0.5:
        warnings.append("Match encontrado com baixa confianca; revisar item e ofertas.")
    if structural_conflict_count(results):
        warnings.append("Algumas ofertas possuem conflito estrutural com o canonical; revisar variante e apresentacao.")
    if results:
        canonical_product = matches[0][1]
        tracked_item = register_tracked_item(
            db, query, requested_cep, "search_products", canonical_product=canonical_product, source_kind="direct_search", match_confidence=confidence
        )
        best_offer = best_pricing_offer(results[0]["offers"])
        if not best_offer and results[0]["offers"]:
            warnings.append("Produto encontrado, mas as ofertas atuais estao sem estoque.")
        elif best_offer and best_offer.get("availability") == "unknown":
            warnings.append("Melhor oferta encontrada com estoque nao confirmado.")
    else:
        tracked_item = register_tracked_item(db, query, requested_cep, "search_products", source_kind="direct_search", match_confidence=0.0)
        catalog_request = register_catalog_request(db, query, requested_cep, "search_products")
        search_job = register_search_job(db, query, requested_cep, "search_products", catalog_request)
        operation_job = _enqueue_search_job_processing(db, search_job=search_job, requested_by="tool_search_products")
        warnings.append("Item registrado para enriquecimento futuro do catalogo.")
        warnings.append("Busca sob demanda adicionada a fila de processamento.")

    catalog_request_result = catalog_request_payload(catalog_request)
    search_job_result = search_job_payload(search_job, db)
    tracked_item_result = tracked_item_payload(tracked_item)
    requires_polling = search_job_result is not None
    result_resolution_source = resolution_source or "queued_enrichment"
    grouped_payload = grouped_results(results)
    freshness_summary = results_freshness_summary(results)
    next_action, next_action_reason = next_action_for_results(results, requires_polling=requires_polling)
    result_refs = flattened_tool_refs(
        catalog_request=catalog_request_result,
        search_job=search_job_result,
        operation_job=operation_job,
        tracked_item=tracked_item_result,
    )

    return tool_response(
        "search_products",
        {"query": query, "cep": cep, "match_mode": normalized_match_mode},
        {
            "match_mode": normalized_match_mode,
            "recommended_match_mode": recommended_mode,
            "next_action": next_action,
            "next_action_reason": next_action_reason,
            "resolution_source": result_resolution_source,
            "outcome": outcome_for_results(results, requires_polling=requires_polling),
            "evidence_level": evidence_level(result_resolution_source, results),
            "requires_polling": requires_polling,
            "results_count": len(results),
            "offers_count": results_offer_count(results),
            "results_with_offers_count": results_with_offers_count(results),
            "freshness_summary": freshness_summary,
            "structural_conflict_count": structural_conflict_count(results),
            "unique_pharmacies_count": len(unique_pharmacies(results)),
            "unique_pharmacies": unique_pharmacies(results),
            "groups": grouped_payload,
            **result_refs,
            "results": results,
            "catalog_request": catalog_request_result,
            "search_job": search_job_result,
            "operation_job": operation_job,
            "tracked_item": tracked_item_result,
        },
        confidence,
        warnings,
    )


def submit_pharmacy_lead_service(payload: PharmacyLeadRequest, db: Session):
    normalized_url, normalized_domain = normalize_website_url(payload.website_url)
    normalized_cep = validate_cep_context(payload.cep) if payload.cep else None
    now = datetime.now(UTC).replace(tzinfo=None)
    lead = db.query(PharmacyLead).filter(PharmacyLead.normalized_domain == normalized_domain).first()

    created = False
    if not lead:
        lead = PharmacyLead(
            pharmacy_name=(payload.pharmacy_name or "").strip() or None,
            website_url=normalized_url,
            normalized_domain=normalized_domain,
            suggested_cep=normalized_cep,
            suggested_city=(payload.city or "").strip() or None,
            suggested_state=((payload.state or "").strip().upper() or None),
            notes=(payload.notes or "").strip() or None,
            status="new",
            suggestion_count=1,
            first_suggested_at=now,
            last_suggested_at=now,
            last_suggested_by_tool="submit_pharmacy_lead",
        )
        db.add(lead)
        created = True
    else:
        lead.website_url = normalized_url
        lead.pharmacy_name = lead.pharmacy_name or ((payload.pharmacy_name or "").strip() or None)
        lead.suggested_cep = normalized_cep or lead.suggested_cep
        lead.suggested_city = (payload.city or "").strip() or lead.suggested_city
        lead.suggested_state = ((payload.state or "").strip().upper() or lead.suggested_state)
        lead.notes = (payload.notes or "").strip() or lead.notes
        lead.suggestion_count = int(lead.suggestion_count or 0) + 1
        lead.last_suggested_at = now
        lead.last_suggested_by_tool = "submit_pharmacy_lead"
        if lead.status in {None, ""}:
            lead.status = "new"

    db.commit()
    db.refresh(lead)

    next_action = "thank_user"
    next_action_reason = "A farmacia foi registrada como sugestao de cobertura futura e nao altera a busca atual."
    warnings = []
    if not normalized_cep:
        warnings.append("A sugestao foi registrada sem CEP; isso reduz a precisao regional do sinal.")
    if not payload.city:
        warnings.append("A sugestao foi registrada sem cidade; cidade continua sendo um contexto util de apoio.")

    return tool_response(
        "submit_pharmacy_lead",
        payload.model_dump(),
        {
            "created": created,
            "next_action": next_action,
            "next_action_reason": next_action_reason,
            "lead": pharmacy_lead_payload(lead),
        },
        1.0,
        warnings,
    )


def compare_shopping_list_service(payload: ShoppingListRequest, db: Session):
    requested_cep = validate_cep_context(payload.cep)
    latest_prices = build_latest_price_map(db, requested_cep)
    cmed_reference_map = build_cmed_reference_map(db)
    comparisons = []
    scores = []
    catalog_requests = []
    search_jobs = []
    operation_jobs = []
    tracked_items = []

    for item in payload.items:
        matches, resolution_source = find_matches_with_resolution_source(db, item, latest_prices, limit=1)
        if not matches:
            comparisons.append({"requested_item": item, "match_found": False, "results": [], "resolution_source": "queued_enrichment"})
            tracked_item = register_tracked_item(db, item, requested_cep, "compare_shopping_list", source_kind="shopping_list", match_confidence=0.0)
            tracked_items.append(tracked_item_payload(tracked_item))
            catalog_request = register_catalog_request(db, item, requested_cep, "compare_shopping_list")
            catalog_requests.append(catalog_request_payload(catalog_request))
            search_job = register_search_job(db, item, requested_cep, "compare_shopping_list", catalog_request)
            search_jobs.append(search_job_payload(search_job, db))
            operation_jobs.append(_enqueue_search_job_processing(db, search_job=search_job, requested_by="tool_compare_shopping_list"))
            continue

        score, canonical_product = matches[0]
        scores.append(score)
        offers = canonical_offer_payload(canonical_product, latest_prices, cmed_reference_map)
        tracked_item = register_tracked_item(
            db, item, requested_cep, "compare_shopping_list", canonical_product=canonical_product, source_kind="shopping_list", match_confidence=min(score / 100, 1.0)
        )
        tracked_items.append(tracked_item_payload(tracked_item))
        comparisons.append(
            {
                "requested_item": item,
                "match_found": True,
                "quantity": 1,
                "score": score,
                "canonical_product_id": canonical_product.id,
                "canonical_name": canonical_product.canonical_name,
                "display_name": canonical_display_name(canonical_product),
                "presentation_group": canonical_presentation_group(canonical_product),
                "best_offer": best_pricing_offer(offers),
                "data_freshness": (best_pricing_offer(offers) or {}).get("data_freshness"),
                "offers": offers,
                "resolution_source": resolution_source,
                "availability_summary": item_availability_summary({"match_found": True, "best_offer": best_pricing_offer(offers), "offers": offers}),
            }
        )

    unmatched_count = len([item for item in comparisons if not item["match_found"]])
    warnings = []
    if unmatched_count:
        warnings.append(f"{unmatched_count} item(ns) da lista nao tiveram match.")
        warnings.append("Itens sem match foram adicionados a fila de busca sob demanda.")
    if any(item.get("match_found") and item.get("score", 0) < 50 for item in comparisons):
        warnings.append("Alguns matches da lista tem baixa confianca.")
    if comparisons and not build_price_summary(comparisons)["best_basket_pharmacy"]:
        warnings.append("Nenhuma farmacia unica cobre toda a cesta atual.")
    warnings.extend(availability_warnings(comparisons))
    ref_lists = flattened_tool_ref_lists(
        catalog_requests=catalog_requests,
        search_jobs=search_jobs,
        operation_jobs=operation_jobs,
        tracked_items=tracked_items,
    )
    requires_polling = bool(ref_lists["search_job_ids"])
    item_results = [item for item in comparisons if item.get("match_found")]
    item_resolution_counts = resolution_source_summary(comparisons)
    grouped_payload = grouped_results(item_results)
    freshness_summary = results_freshness_summary(item_results)
    next_action, next_action_reason = next_action_for_results(item_results, requires_polling=requires_polling)
    if freshness_summary["quality"] == "expired_only":
        warnings.append("Os melhores precos encontrados para a cesta estao expirados; responda com cautela.")

    return tool_response(
        "compare_shopping_list",
        payload.model_dump(),
        {
            **build_basket_result(comparisons),
            "next_action": next_action,
            "next_action_reason": next_action_reason,
            "outcome": outcome_for_results(item_results, requires_polling=requires_polling),
            "evidence_level": evidence_level(
                "source_product_fallback" if item_resolution_counts.get("source_product_fallback") else "canonical_match" if item_results else None,
                item_results,
            ),
            "requires_polling": requires_polling,
            "results_count": len(item_results),
            "offers_count": results_offer_count(item_results),
            "results_with_offers_count": results_with_offers_count(item_results),
            "freshness_summary": freshness_summary,
            "unique_pharmacies_count": len(unique_pharmacies(item_results)),
            "unique_pharmacies": unique_pharmacies(item_results),
            "groups": grouped_payload,
            "resolution_source_summary": item_resolution_counts,
            **ref_lists,
            "catalog_requests": catalog_requests,
            "search_jobs": search_jobs,
            "operation_jobs": operation_jobs,
            "tracked_items": tracked_items,
        },
        min((sum(scores) / len(scores)) / 100, 1.0) if scores else 0.0,
        warnings,
    )


def compare_basket_service(payload: ShoppingListRequest, db: Session):
    result = compare_shopping_list_service(payload, db)
    result["tool_name"] = "compare_basket"
    return result


def compare_invoice_items_service(payload: InvoiceComparisonRequest, db: Session):
    requested_cep = validate_cep_context(payload.cep)
    latest_prices = build_latest_price_map(db, requested_cep)
    cmed_reference_map = build_cmed_reference_map(db)
    comparisons = []
    max_score = 0
    catalog_requests = []
    search_jobs = []
    operation_jobs = []
    tracked_items = []

    for item in payload.items:
        matches, resolution_source = find_matches_with_resolution_source(db, item.description, latest_prices, limit=1)
        if not matches:
            comparisons.append(
                {
                    "invoice_item": item.description,
                    "paid_price": item.paid_price,
                    "quantity": item.quantity,
                    "match_found": False,
                    "potential_savings": None,
                    "results": [],
                    "resolution_source": "queued_enrichment",
                }
            )
            tracked_item = register_tracked_item(db, item.description, requested_cep, "compare_invoice_items", source_kind="invoice_item", match_confidence=0.0)
            tracked_items.append(tracked_item_payload(tracked_item))
            catalog_request = register_catalog_request(db, item.description, requested_cep, "compare_invoice_items")
            catalog_requests.append(catalog_request_payload(catalog_request))
            search_job = register_search_job(db, item.description, requested_cep, "compare_invoice_items", catalog_request)
            search_jobs.append(search_job_payload(search_job, db))
            operation_jobs.append(_enqueue_search_job_processing(db, search_job=search_job, requested_by="tool_compare_invoice_items"))
            continue

        score, canonical_product = matches[0]
        max_score = max(max_score, score)
        offers = canonical_offer_payload(canonical_product, latest_prices, cmed_reference_map)
        best_offer = best_pricing_offer(offers)
        tracked_item = register_tracked_item(
            db, item.description, requested_cep, "compare_invoice_items", canonical_product=canonical_product, source_kind="invoice_item", match_confidence=min(score / 100, 1.0)
        )
        tracked_items.append(tracked_item_payload(tracked_item))
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
                "display_name": canonical_display_name(canonical_product),
                "presentation_group": canonical_presentation_group(canonical_product),
                "best_offer": best_offer,
                "data_freshness": (best_offer or {}).get("data_freshness"),
                "potential_savings": potential_savings,
                "offers": offers,
                "resolution_source": resolution_source,
                "availability_summary": item_availability_summary({"match_found": True, "best_offer": best_offer, "offers": offers}),
            }
        )

    total_potential_savings = round(sum(item["potential_savings"] or 0 for item in comparisons), 2)
    unmatched_count = len([item for item in comparisons if not item["match_found"]])
    warnings = []
    if unmatched_count:
        warnings.append(f"{unmatched_count} item(ns) da nota nao tiveram match.")
        warnings.append("Itens sem match foram adicionados a fila de busca sob demanda.")
    if comparisons and max_score < 50:
        warnings.append("Alguns matches da nota tem baixa confianca.")
    warnings.extend(availability_warnings(comparisons))
    ref_lists = flattened_tool_ref_lists(
        catalog_requests=catalog_requests,
        search_jobs=search_jobs,
        operation_jobs=operation_jobs,
        tracked_items=tracked_items,
    )
    requires_polling = bool(ref_lists["search_job_ids"])
    item_results = [item for item in comparisons if item.get("match_found")]
    item_resolution_counts = resolution_source_summary(comparisons)
    grouped_payload = grouped_results(item_results)
    freshness_summary = results_freshness_summary(item_results)
    next_action, next_action_reason = next_action_for_results(item_results, requires_polling=requires_polling)
    if freshness_summary["quality"] == "expired_only":
        warnings.append("Os melhores precos comparados na nota estao expirados; responda com cautela.")

    return tool_response(
        "compare_invoice_items",
        payload.model_dump(),
        {
            "items": comparisons,
            "next_action": next_action,
            "next_action_reason": next_action_reason,
            "outcome": outcome_for_results(item_results, requires_polling=requires_polling),
            "evidence_level": evidence_level(
                "source_product_fallback" if item_resolution_counts.get("source_product_fallback") else "canonical_match" if item_results else None,
                item_results,
            ),
            "requires_polling": requires_polling,
            "results_count": len(item_results),
            "offers_count": results_offer_count(item_results),
            "results_with_offers_count": results_with_offers_count(item_results),
            "freshness_summary": freshness_summary,
            "unique_pharmacies_count": len(unique_pharmacies(item_results)),
            "unique_pharmacies": unique_pharmacies(item_results),
            "groups": grouped_payload,
            "resolution_source_summary": item_resolution_counts,
            **ref_lists,
            "total_potential_savings": total_potential_savings,
            "catalog_requests": catalog_requests,
            "search_jobs": search_jobs,
            "operation_jobs": operation_jobs,
            "tracked_items": tracked_items,
        },
        min(max_score / 100, 1.0) if comparisons else 0.0,
        warnings,
    )


def compare_receipt_service(payload: ReceiptComparisonRequest, db: Session):
    validate_cep_context(payload.cep)
    invoice_result = compare_invoice_items_service(InvoiceComparisonRequest(cep=payload.cep, items=payload.items), db)
    items = invoice_result["result"]["items"]
    summary = build_price_summary(items)
    warnings = list(invoice_result["warnings"])
    if not items:
        warnings.append("Nenhum item foi enviado para comparacao da nota.")
    result_payload = invoice_result["result"]
    return tool_response(
        "compare_receipt",
        payload.model_dump(),
        {
            "merchant_name": payload.merchant_name,
            "captured_at": payload.captured_at,
            **build_basket_result(items),
            "summary": summary,
            "next_action": result_payload.get("next_action"),
            "next_action_reason": result_payload.get("next_action_reason"),
            "outcome": result_payload.get("outcome"),
            "evidence_level": result_payload.get("evidence_level"),
            "requires_polling": result_payload.get("requires_polling"),
            "results_count": result_payload.get("results_count"),
            "offers_count": result_payload.get("offers_count"),
            "results_with_offers_count": result_payload.get("results_with_offers_count"),
            "freshness_summary": result_payload.get("freshness_summary"),
            "unique_pharmacies_count": result_payload.get("unique_pharmacies_count"),
            "unique_pharmacies": result_payload.get("unique_pharmacies", []),
            "groups": result_payload.get("groups", []),
            "resolution_source_summary": result_payload.get("resolution_source_summary", resolution_source_summary(items)),
            "catalog_request_ids": result_payload.get("catalog_request_ids", []),
            "search_job_ids": result_payload.get("search_job_ids", []),
            "operation_job_ids": result_payload.get("operation_job_ids", []),
            "tracked_item_ids": result_payload.get("tracked_item_ids", []),
            "catalog_requests": result_payload.get("catalog_requests", []),
            "search_jobs": result_payload.get("search_jobs", []),
            "operation_jobs": result_payload.get("operation_jobs", []),
            "tracked_items": result_payload.get("tracked_items", []),
        },
        estimate_overall_confidence(items),
        warnings,
    )


def search_observed_item_service(payload: ObservedItemRequest, db: Session):
    requested_cep = validate_cep_context(payload.cep)
    query = build_observed_query(payload)
    latest_prices = build_latest_price_map(db, requested_cep)
    cmed_reference_map = build_cmed_reference_map(db)
    matches, resolution_source = find_matches_with_resolution_source(db, query, latest_prices)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "display_name": canonical_display_name(canonical_product),
            "presentation_group": canonical_presentation_group(canonical_product),
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": offers,
            "data_freshness": (best_pricing_offer(offers) or {}).get("data_freshness"),
            "availability_summary": item_availability_summary({"match_found": True, "best_offer": best_pricing_offer(offers), "offers": offers}),
        }
        for score, canonical_product in matches
        for offers in [canonical_offer_payload(canonical_product, latest_prices, cmed_reference_map)]
    ]

    warnings = []
    catalog_request = None
    search_job = None
    operation_job = None
    tracked_item = None
    if payload.source_type == "box_photo":
        warnings.append("Entrada tratada como OCR de caixa; revise lote, validade e textos promocionais ignorados.")
    if not results:
        warnings.append("Nenhum produto canonico encontrado a partir das observacoes enviadas.")
        tracked_item = register_tracked_item(db, query, requested_cep, "search_observed_item", source_kind=payload.source_type, match_confidence=0.0)
        catalog_request = register_catalog_request(db, query, requested_cep, "search_observed_item")
        search_job = register_search_job(db, query, requested_cep, "search_observed_item", catalog_request)
        operation_job = _enqueue_search_job_processing(db, search_job=search_job, requested_by="tool_search_observed_item")
        warnings.append("Item observado registrado para enriquecimento futuro do catalogo.")
        warnings.append("Busca sob demanda adicionada a fila de processamento.")
    elif results[0]["score"] < 50:
        tracked_item = register_tracked_item(db, query, requested_cep, "search_observed_item", canonical_product=matches[0][1], source_kind=payload.source_type, match_confidence=min(results[0]["score"] / 100, 1.0))
        warnings.append("Match encontrado com baixa confianca para item observado.")
    else:
        tracked_item = register_tracked_item(db, query, requested_cep, "search_observed_item", canonical_product=matches[0][1], source_kind=payload.source_type, match_confidence=min(results[0]["score"] / 100, 1.0))
        best_offer = best_pricing_offer(results[0]["offers"])
        if not best_offer and results[0]["offers"]:
            warnings.append("Produto encontrado, mas as ofertas atuais estao sem estoque.")
        elif best_offer and best_offer.get("availability") == "unknown":
            warnings.append("Melhor oferta encontrada com estoque nao confirmado para item observado.")

    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    catalog_request_result = catalog_request_payload(catalog_request)
    search_job_result = search_job_payload(search_job, db)
    tracked_item_result = tracked_item_payload(tracked_item)
    requires_polling = search_job_result is not None
    result_resolution_source = resolution_source or "queued_enrichment"
    grouped_payload = grouped_results(results)
    freshness_summary = results_freshness_summary(results)
    next_action, next_action_reason = next_action_for_results(results, requires_polling=requires_polling)
    result_refs = flattened_tool_refs(
        catalog_request=catalog_request_result,
        search_job=search_job_result,
        operation_job=operation_job,
        tracked_item=tracked_item_result,
    )
    return tool_response(
        "search_observed_item",
        payload.model_dump(),
        {
            "normalized_query": query,
            "next_action": next_action,
            "next_action_reason": next_action_reason,
            "resolution_source": result_resolution_source,
            "outcome": outcome_for_results(results, requires_polling=requires_polling),
            "evidence_level": evidence_level(result_resolution_source, results),
            "requires_polling": requires_polling,
            "results_count": len(results),
            "offers_count": results_offer_count(results),
            "results_with_offers_count": results_with_offers_count(results),
            "freshness_summary": freshness_summary,
            "unique_pharmacies_count": len(unique_pharmacies(results)),
            "unique_pharmacies": unique_pharmacies(results),
            "groups": grouped_payload,
            **result_refs,
            "results": results,
            "catalog_request": catalog_request_result,
            "search_job": search_job_result,
            "operation_job": operation_job,
            "tracked_item": tracked_item_result,
        },
        confidence,
        warnings,
    )


def compare_canonical_product_service(canonical_product_id: int, cep: str, db: Session):
    requested_cep = validate_cep_context(cep)
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
        raise ValueError("Produto canonico nao encontrado")
    latest_prices = build_latest_price_map(db, requested_cep)
    offers = canonical_offer_payload(canonical_product, latest_prices, build_cmed_reference_map(db))
    return {
        "canonical_product_id": canonical_product.id,
        "canonical_name": canonical_product.canonical_name,
        "display_name": canonical_display_name(canonical_product),
        "presentation_group": canonical_presentation_group(canonical_product),
        "ean_gtin": canonical_product.ean_gtin,
        "anvisa_code": canonical_product.anvisa_code,
        "cep": requested_cep,
        "outcome": "resolved",
        "evidence_level": evidence_level("canonical_match", [{"offers": offers}]),
        "requires_polling": False,
        "results_count": 1,
        "offers_count": len(offers),
        "results_with_offers_count": 1 if offers else 0,
        "unique_pharmacies_count": len(unique_pharmacies([{"offers": offers}])),
        "unique_pharmacies": unique_pharmacies([{"offers": offers}]),
        "resolution_source": "canonical_match",
        "offers": offers,
    }


def list_review_matches_service(db: Session, *, cep: str | None = None, limit: int = 100, offset: int = 0):
    normalized_cep = validate_cep_context(cep) if cep else None
    query = (
        db.query(SourceProduct)
        .options(
            joinedload(SourceProduct.pharmacy),
            joinedload(SourceProduct.match).joinedload(ProductMatch.canonical_product),
        )
        .join(ProductMatch, ProductMatch.source_product_id == SourceProduct.id)
        .filter(ProductMatch.review_status == "needs_review")
    )
    if normalized_cep:
        source_product_ids = list(build_latest_price_map(db, normalized_cep).keys())
        if not source_product_ids:
            return []
        query = query.filter(SourceProduct.id.in_(source_product_ids))
    products = query.order_by(ProductMatch.confidence.asc(), SourceProduct.id.desc()).offset(offset).limit(limit).all()
    return [
        {
            "source_product_id": product.id,
            "pharmacy": product.pharmacy.name,
            "raw_name": product.raw_name,
            "ean_gtin": product.ean_gtin,
            "anvisa_code": product.anvisa_code,
            "requested_cep": normalized_cep,
            "canonical_product_id": product.match.canonical_product_id,
            "canonical_name": product.match.canonical_product.canonical_name if product.match and product.match.canonical_product else None,
            "match_type": product.match.match_type,
            "match_confidence": product.match.confidence,
            "review_notes": product.match.review_notes,
        }
        for product in products
    ]
