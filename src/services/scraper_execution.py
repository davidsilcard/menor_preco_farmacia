import asyncio
import inspect


def _chunked(values: list[str], chunk_size: int):
    if chunk_size <= 0:
        chunk_size = len(values) or 1
    for index in range(0, len(values), chunk_size):
        yield values[index : index + chunk_size]


def _run_scraper_once(scraper, terms: list[str], cep: str):
    original_terms = list(getattr(scraper, "search_terms", []) or [])
    original_cep = getattr(scraper, "cep", None)
    scraper.search_terms = list(terms)
    scraper.set_cep(cep)
    try:
        scrape_method = scraper.scrape
        if inspect.iscoroutinefunction(scrape_method):
            products = asyncio.run(scrape_method())
        else:
            products = scrape_method()
        products = products or []
        if products:
            scraper.save_to_db(products)
        return products
    finally:
        scraper.search_terms = original_terms
        if original_cep:
            scraper.cep = original_cep


def run_scraper_terms_with_fallback(scraper, terms: list[str], cep: str, *, batch_size: int):
    unique_terms = list(dict.fromkeys(term for term in (terms or []) if term))
    if not unique_terms:
        return {
            "status": "skipped",
            "products_found": 0,
            "attempted_terms": [],
            "processed_terms": [],
            "failed_terms": [],
            "fallback_applied": False,
        }

    products_found = 0
    processed_terms: list[str] = []
    failed_terms: list[str] = []
    fallback_applied = False

    for chunk in _chunked(unique_terms, batch_size):
        try:
            products = _run_scraper_once(scraper, chunk, cep)
            products_found += len(products)
            processed_terms.extend(chunk)
            continue
        except Exception:
            if len(chunk) == 1:
                failed_terms.extend(chunk)
                continue

        fallback_applied = True
        for term in chunk:
            try:
                products = _run_scraper_once(scraper, [term], cep)
                products_found += len(products)
                processed_terms.append(term)
            except Exception:
                failed_terms.append(term)

    if processed_terms and not failed_terms:
        status = "completed"
    elif processed_terms:
        status = "partial_success"
    else:
        status = "failed"

    return {
        "status": status,
        "products_found": products_found,
        "attempted_terms": unique_terms,
        "processed_terms": processed_terms,
        "failed_terms": failed_terms,
        "fallback_applied": fallback_applied,
    }
