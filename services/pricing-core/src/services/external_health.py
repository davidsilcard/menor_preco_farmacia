import json
from datetime import UTC, datetime
from time import perf_counter
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from src.core.config import settings
from src.models.base import ScrapeRun
from src.services.catalog_queries import FRESH_DATA_MAX_AGE_MINUTES, STALE_DATA_MAX_AGE_MINUTES, data_age_minutes
from src.services.ops import pharmacy_metrics
from src.services.scraper_registry import SCRAPER_REGISTRY

def _browser_runtime_enabled():
    return settings.ON_DEMAND_ENABLE_BROWSER_SCRAPERS or settings.SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS


def scraper_registry_payload():
    entries = []
    for slug, runtime_type, scraper_cls in SCRAPER_REGISTRY:
        scraper = scraper_cls()
        base_domain = getattr(scraper, "base_domain", getattr(scraper, "base_url", None))
        entries.append(
            {
                "pharmacy_slug": scraper.pharmacy_slug or slug,
                "pharmacy": scraper.pharmacy_name,
                "runtime": getattr(scraper, "runtime_type", runtime_type),
                "runtime_enabled": scraper.runtime_enabled() if hasattr(scraper, "runtime_enabled") else (True if runtime_type == "http" else _browser_runtime_enabled()),
                "base_domain": base_domain,
                "scraper_class": scraper_cls.__name__,
            }
        )
    return entries


def scraper_health_payload(db, *, registry_entries=None, cep: str | None = None):
    registry_entries = registry_entries or scraper_registry_payload()
    metrics_by_name = pharmacy_metrics(db, cep=cep)
    runs_query = db.query(ScrapeRun)
    if cep:
        runs_query = runs_query.filter(ScrapeRun.cep == cep)
    runs = runs_query.order_by(
        ScrapeRun.started_at.desc(),
        ScrapeRun.id.desc(),
    ).all()

    display_name_by_slug = {entry["pharmacy_slug"]: entry["pharmacy"] for entry in registry_entries}
    for run in runs:
        pharmacy = getattr(run, "pharmacy", None)
        slug = getattr(pharmacy, "slug", None)
        name = getattr(pharmacy, "name", None)
        if slug and name:
            display_name_by_slug[slug] = name

    payload = []
    for entry in registry_entries:
        slug = entry["pharmacy_slug"]
        pharmacy_name = display_name_by_slug.get(slug, entry["pharmacy"])
        run_list = [run for run in runs if getattr(getattr(run, "pharmacy", None), "slug", None) == slug]
        latest_run = run_list[0] if run_list else None
        last_success = next((run for run in run_list if run.status == "completed"), None)
        last_failure = next((run for run in run_list if run.status == "failed"), None)

        failure_streak = 0
        for run in run_list:
            if run.status == "failed":
                failure_streak += 1
                continue
            break

        metrics = metrics_by_name.get(pharmacy_name, {})
        latest_snapshot_age = metrics.get("latest_snapshot_age_minutes")
        last_success_age = data_age_minutes(getattr(last_success, "finished_at", None) or getattr(last_success, "started_at", None))
        recent_statuses = [run.status for run in run_list[:3]]

        status = "unknown"
        if not run_list:
            status = "unknown"
        elif getattr(latest_run, "status", None) == "failed" or failure_streak >= 2:
            status = "degraded"
        elif latest_snapshot_age is None:
            status = "attention"
        elif latest_snapshot_age > STALE_DATA_MAX_AGE_MINUTES or (last_success_age is not None and last_success_age > STALE_DATA_MAX_AGE_MINUTES):
            status = "attention"
        elif latest_snapshot_age <= FRESH_DATA_MAX_AGE_MINUTES:
            status = "healthy"
        else:
            status = "attention"

        payload.append(
            {
                "pharmacy_slug": slug,
                "pharmacy": pharmacy_name,
                "runtime": entry["runtime"],
                "runtime_enabled": entry["runtime_enabled"],
                "status": status,
                "recent_statuses": recent_statuses,
                "failure_streak": failure_streak,
                "last_run_status": getattr(latest_run, "status", None),
                "last_run_started_at": getattr(latest_run, "started_at", None),
                "last_run_finished_at": getattr(latest_run, "finished_at", None),
                "last_success_at": getattr(last_success, "finished_at", None) or getattr(last_success, "started_at", None),
                "last_failure_at": getattr(last_failure, "finished_at", None) or getattr(last_failure, "started_at", None),
                "last_success_age_minutes": last_success_age,
                "latest_snapshot_age_minutes": latest_snapshot_age,
                "source_products": metrics.get("source_products", 0),
                "matched_products": metrics.get("matched_products", 0),
                "match_rate": metrics.get("match_rate", 0.0),
                "availability_counts": metrics.get("availability_counts", {"available": 0, "unknown": 0, "out_of_stock": 0}),
            }
        )

    degraded = sum(1 for item in payload if item["status"] == "degraded")
    attention = sum(1 for item in payload if item["status"] == "attention")
    overall_status = "healthy"
    if degraded:
        overall_status = "degraded"
    elif attention:
        overall_status = "attention"

    return {
        "status": overall_status,
        "generated_at": datetime.now(UTC).replace(tzinfo=None),
        "requested_cep": cep,
        "summary": {
            "pharmacies_total": len(payload),
            "healthy": sum(1 for item in payload if item["status"] == "healthy"),
            "attention": attention,
            "degraded": degraded,
            "unknown": sum(1 for item in payload if item["status"] == "unknown"),
        },
        "pharmacies": payload,
    }

def build_page_probe_specs(*, sample_term: str = "dipirona", registry_entries=None):
    specs = []
    for slug, runtime_type, scraper_cls in SCRAPER_REGISTRY:
        scraper = scraper_cls()
        specs.append(
            {
                "pharmacy_slug": scraper.pharmacy_slug or slug,
                "pharmacy": scraper.pharmacy_name,
                "runtime": getattr(scraper, "runtime_type", runtime_type),
                "runtime_enabled": scraper.runtime_enabled() if hasattr(scraper, "runtime_enabled") else (True if runtime_type == "http" else _browser_runtime_enabled()),
                "probes": scraper.build_probe_specs(sample_term=sample_term),
            }
        )
    return specs


def _default_probe_fetcher(spec: dict, timeout_seconds: int):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
    }
    request = Request(spec["url"], headers=headers, method=spec.get("method", "GET"))
    started_at = perf_counter()
    with urlopen(request, timeout=timeout_seconds) as response:
        latency_ms = round((perf_counter() - started_at) * 1000, 2)
        body_bytes = response.read(4096)
        body_text = body_bytes.decode("utf-8", "ignore")
        content_type = response.headers.get("Content-Type", "")
        return {
            "status_code": response.getcode(),
            "content_type": content_type,
            "body_text": body_text,
            "latency_ms": latency_ms,
        }


def _evaluate_probe(spec: dict, response: dict):
    issues = []
    status_code = int(response.get("status_code", 0) or 0)
    content_type = (response.get("content_type") or "").lower()
    body_text = response.get("body_text") or ""
    body_lower = body_text.lower()

    if status_code < 200 or status_code >= 400:
        issues.append(f"Status HTTP inesperado: {status_code}.")

    expected_content_type = (spec.get("expected_content_type") or "").lower()
    if expected_content_type and expected_content_type not in content_type:
        issues.append(f"Content-Type inesperado: {content_type or 'ausente'}.")

    contains_any = [token.lower() for token in spec.get("contains_any", []) if token]
    if contains_any and not any(token in body_lower for token in contains_any):
        issues.append("Resposta nao contem nenhum marcador esperado.")

    expected_json_root = spec.get("expected_json_root")
    if expected_json_root:
        try:
            parsed = json.loads(body_text)
        except json.JSONDecodeError:
            issues.append("Resposta nao e JSON valido.")
        else:
            if expected_json_root == "list" and not isinstance(parsed, list):
                issues.append("JSON nao retornou lista na raiz.")
            if expected_json_root == "dict" and not isinstance(parsed, dict):
                issues.append("JSON nao retornou objeto na raiz.")

    return {
        "ok": not issues,
        "issues": issues,
        "status_code": status_code,
        "content_type": response.get("content_type"),
        "latency_ms": response.get("latency_ms"),
    }


def page_health_payload(
    *,
    sample_term: str = "dipirona",
    timeout_seconds: int = 8,
    fetcher=None,
    probe_specs=None,
):
    fetcher = fetcher or _default_probe_fetcher
    probe_specs = probe_specs or build_page_probe_specs(sample_term=sample_term)

    pharmacies = []
    failed_probes = 0
    total_probes = 0
    for pharmacy in probe_specs:
        probe_payloads = []
        for spec in pharmacy.get("probes", []):
            total_probes += 1
            try:
                response = fetcher(spec, timeout_seconds)
                probe_result = _evaluate_probe(spec, response)
            except HTTPError as exc:
                probe_result = {
                    "ok": False,
                    "issues": [f"HTTPError: {exc.code}"],
                    "status_code": exc.code,
                    "content_type": getattr(exc, "headers", {}).get("Content-Type") if getattr(exc, "headers", None) else None,
                    "latency_ms": None,
                }
            except URLError as exc:
                probe_result = {
                    "ok": False,
                    "issues": [f"URLError: {exc.reason}"],
                    "status_code": None,
                    "content_type": None,
                    "latency_ms": None,
                }
            except Exception as exc:
                probe_result = {
                    "ok": False,
                    "issues": [str(exc)],
                    "status_code": None,
                    "content_type": None,
                    "latency_ms": None,
                }

            if not probe_result["ok"]:
                failed_probes += 1

            probe_payloads.append(
                {
                    "probe_name": spec["probe_name"],
                    "url": spec["url"],
                    **probe_result,
                }
            )

        pharmacy_status = "healthy" if probe_payloads and all(probe["ok"] for probe in probe_payloads) else "degraded"
        pharmacies.append(
            {
                "pharmacy_slug": pharmacy["pharmacy_slug"],
                "pharmacy": pharmacy["pharmacy"],
                "runtime": pharmacy["runtime"],
                "runtime_enabled": pharmacy["runtime_enabled"],
                "status": pharmacy_status,
                "probes": probe_payloads,
            }
        )

    overall_status = "healthy" if failed_probes == 0 else "degraded"
    return {
        "status": overall_status,
        "generated_at": datetime.now(UTC).replace(tzinfo=None),
        "sample_term": sample_term,
        "timeout_seconds": timeout_seconds,
        "summary": {
            "pharmacies_total": len(pharmacies),
            "probes_total": total_probes,
            "probes_failed": failed_probes,
            "pharmacies_healthy": sum(1 for pharmacy in pharmacies if pharmacy["status"] == "healthy"),
            "pharmacies_degraded": sum(1 for pharmacy in pharmacies if pharmacy["status"] == "degraded"),
        },
        "pharmacies": pharmacies,
    }
