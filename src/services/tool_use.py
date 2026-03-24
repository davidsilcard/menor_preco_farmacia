import re

from sqlalchemy.orm import Session, joinedload

from src.models.base import CanonicalProduct, ProductMatch, SourceProduct
from src.services.catalog_queries import (
    best_pricing_offer,
    build_latest_price_map,
    canonical_offer_payload,
    find_matching_canonicals,
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


def search_products_service(query: str, cep: str, db: Session):
    requested_cep = validate_cep_context(cep)
    latest_prices = build_latest_price_map(db, requested_cep)
    matches = find_matching_canonicals(db, query)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
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
        for offers in [canonical_offer_payload(canonical_product, latest_prices)]
    ]
    confidence = min(results[0]["score"] / 100, 1.0) if results else 0.0
    catalog_request = None
    search_job = None
    operation_job = None
    tracked_item = None
    warnings = [] if results else ["Nenhum produto canonico encontrado para a consulta."]
    if results and confidence < 0.5:
        warnings.append("Match encontrado com baixa confianca; revisar item e ofertas.")
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

    return tool_response(
        "search_products",
        {"query": query, "cep": cep},
        {
            "results": results,
            "catalog_request": catalog_request_payload(catalog_request),
            "search_job": search_job_payload(search_job, db),
            "operation_job": operation_job,
            "tracked_item": tracked_item_payload(tracked_item),
        },
        confidence,
        warnings,
    )


def compare_shopping_list_service(payload: ShoppingListRequest, db: Session):
    requested_cep = validate_cep_context(payload.cep)
    latest_prices = build_latest_price_map(db, requested_cep)
    comparisons = []
    scores = []
    catalog_requests = []
    search_jobs = []
    operation_jobs = []
    tracked_items = []

    for item in payload.items:
        matches = find_matching_canonicals(db, item, limit=1)
        if not matches:
            comparisons.append({"requested_item": item, "match_found": False, "results": []})
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
        offers = canonical_offer_payload(canonical_product, latest_prices)
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
                "best_offer": best_pricing_offer(offers),
                "data_freshness": (best_pricing_offer(offers) or {}).get("data_freshness"),
                "offers": offers,
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

    return tool_response(
        "compare_shopping_list",
        payload.model_dump(),
        {
            **build_basket_result(comparisons),
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
    comparisons = []
    max_score = 0
    catalog_requests = []
    search_jobs = []
    operation_jobs = []
    tracked_items = []

    for item in payload.items:
        matches = find_matching_canonicals(db, item.description, limit=1)
        if not matches:
            comparisons.append({"invoice_item": item.description, "paid_price": item.paid_price, "quantity": item.quantity, "match_found": False, "potential_savings": None, "results": []})
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
        offers = canonical_offer_payload(canonical_product, latest_prices)
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
                "best_offer": best_offer,
                "data_freshness": (best_offer or {}).get("data_freshness"),
                "potential_savings": potential_savings,
                "offers": offers,
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

    return tool_response(
        "compare_invoice_items",
        payload.model_dump(),
        {
            "items": comparisons,
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
    return tool_response(
        "compare_receipt",
        payload.model_dump(),
        {
            "merchant_name": payload.merchant_name,
            "captured_at": payload.captured_at,
            **build_basket_result(items),
            "summary": summary,
            "catalog_requests": invoice_result["result"].get("catalog_requests", []),
            "search_jobs": invoice_result["result"].get("search_jobs", []),
            "operation_jobs": invoice_result["result"].get("operation_jobs", []),
            "tracked_items": invoice_result["result"].get("tracked_items", []),
        },
        estimate_overall_confidence(items),
        warnings,
    )


def search_observed_item_service(payload: ObservedItemRequest, db: Session):
    requested_cep = validate_cep_context(payload.cep)
    query = build_observed_query(payload)
    latest_prices = build_latest_price_map(db, requested_cep)
    matches = find_matching_canonicals(db, query)
    results = [
        {
            "canonical_product_id": canonical_product.id,
            "canonical_name": canonical_product.canonical_name,
            "ean_gtin": canonical_product.ean_gtin,
            "anvisa_code": canonical_product.anvisa_code,
            "score": score,
            "offers": offers,
            "data_freshness": (best_pricing_offer(offers) or {}).get("data_freshness"),
            "availability_summary": item_availability_summary({"match_found": True, "best_offer": best_pricing_offer(offers), "offers": offers}),
        }
        for score, canonical_product in matches
        for offers in [canonical_offer_payload(canonical_product, latest_prices)]
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
    return tool_response(
        "search_observed_item",
        payload.model_dump(),
        {
            "normalized_query": query,
            "results": results,
            "catalog_request": catalog_request_payload(catalog_request),
            "search_job": search_job_payload(search_job, db),
            "operation_job": operation_job,
            "tracked_item": tracked_item_payload(tracked_item),
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
    offers = canonical_offer_payload(canonical_product, latest_prices)
    return {
        "canonical_product_id": canonical_product.id,
        "canonical_name": canonical_product.canonical_name,
        "ean_gtin": canonical_product.ean_gtin,
        "anvisa_code": canonical_product.anvisa_code,
        "cep": requested_cep,
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
