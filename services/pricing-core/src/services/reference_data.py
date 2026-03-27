import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import delete

from src.core.logging import get_logger, log_event
from src.models.base import CmedPriceEntry, RegulatoryAlias, RegulatoryProduct
from src.scrapers.base import BaseScraper

LOGGER = get_logger(__name__)


@dataclass
class ImportSummary:
    dataset: str
    file_path: str
    rows_read: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    replaced: bool = False


def _now_naive():
    return datetime.now(UTC).replace(tzinfo=None)


def _normalize_text(value):
    return BaseScraper.normalize_text(str(value or ""))


def _normalize_identifier(value, valid_lengths=(8, 12, 13, 14)):
    return BaseScraper.clean_identifier(value, valid_lengths=valid_lengths)


def _normalize_money(value):
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _normalize_columns(row):
    normalized = {}
    for key, value in (row or {}).items():
        normalized_key = _normalize_text(key).replace(" ", "_")
        normalized[normalized_key] = value
    return normalized


def _load_rows(file_path):
    path = Path(file_path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            if "rows" in payload and isinstance(payload["rows"], list):
                return payload["rows"]
            return [payload]
        return payload
    if suffix in {".csv", ".txt"}:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return list(csv.DictReader(handle))
    raise ValueError(f"Formato de arquivo nao suportado: {path}")


def _replace_dataset(session, model):
    session.execute(delete(model))


def _regulatory_external_id(row):
    return (
        str(row.get("external_id") or "").strip()
        or str(row.get("anvisa_code") or "").strip()
        or str(row.get("ean_gtin") or "").strip()
        or _normalize_text(row.get("product_name"))
    )


def _normalize_regulatory_row(row):
    row = _normalize_columns(row)
    product_name = (
        row.get("product_name")
        or row.get("medicamento")
        or row.get("nome_do_produto")
        or row.get("nome_produto")
        or row.get("nome_comercial")
    )
    if not product_name:
        return None

    return {
        "source_system": str(row.get("source_system") or "anvisa").strip() or "anvisa",
        "external_id": _regulatory_external_id(
            {
                "external_id": row.get("external_id"),
                "anvisa_code": row.get("anvisa_code") or row.get("registro_ms") or row.get("registro_anvisa"),
                "ean_gtin": row.get("ean_gtin") or row.get("ean") or row.get("gtin"),
                "product_name": product_name,
            }
        ),
        "product_name": str(product_name).strip(),
        "normalized_product_name": _normalize_text(product_name),
        "dcb_name": str(row.get("dcb_name") or row.get("principio_ativo") or row.get("denominacao_comum_brasileira") or "").strip() or None,
        "active_ingredient": str(row.get("active_ingredient") or row.get("principio_ativo") or "").strip() or None,
        "concentration": str(row.get("concentration") or row.get("concentracao") or "").strip() or None,
        "dosage": str(row.get("dosage") or row.get("dosagem") or "").strip() or None,
        "dosage_form": str(row.get("dosage_form") or row.get("forma_farmaceutica") or "").strip() or None,
        "presentation": str(row.get("presentation") or row.get("apresentacao") or "").strip() or None,
        "route": str(row.get("route") or row.get("via_administracao") or "").strip() or None,
        "manufacturer": str(row.get("manufacturer") or row.get("fabricante") or row.get("laboratorio") or "").strip() or None,
        "registration_holder": str(row.get("registration_holder") or row.get("detentor_registro") or "").strip() or None,
        "ean_gtin": _normalize_identifier(row.get("ean_gtin") or row.get("ean") or row.get("gtin")),
        "anvisa_code": _normalize_identifier(row.get("anvisa_code") or row.get("registro_ms") or row.get("registro_anvisa"), valid_lengths=(8, 9, 10, 11, 12, 13)),
        "source_url": str(row.get("source_url") or row.get("url") or "").strip() or None,
        "source_payload": row,
    }


def _normalize_dcb_alias_row(row):
    row = _normalize_columns(row)
    dcb_name = row.get("dcb_name") or row.get("denominacao_comum_brasileira") or row.get("principio_ativo")
    alias = row.get("alias") or row.get("nome_comercial") or row.get("termo")
    if not dcb_name or not alias:
        return None
    return {
        "alias_type": str(row.get("alias_type") or "dcb").strip() or "dcb",
        "dcb_name": str(dcb_name).strip(),
        "alias": str(alias).strip(),
        "normalized_alias": _normalize_text(alias),
        "source_system": str(row.get("source_system") or "anvisa").strip() or "anvisa",
        "source_payload": row,
    }


def _cmed_fingerprint(row):
    parts = [
        str(row.get("anvisa_code") or ""),
        str(row.get("ean_gtin") or ""),
        str(row.get("product_name") or ""),
        str(row.get("presentation") or ""),
        str(row.get("tax_rate") or ""),
    ]
    return hashlib.sha1("|".join(parts).encode("utf-8")).hexdigest()


def _normalize_cmed_row(row):
    row = _normalize_columns(row)
    product_name = row.get("product_name") or row.get("medicamento") or row.get("produto")
    if not product_name:
        return None
    normalized = {
        "source_dataset": str(row.get("source_dataset") or "cmed").strip() or "cmed",
        "product_name": str(product_name).strip(),
        "normalized_product_name": _normalize_text(product_name),
        "presentation": str(row.get("presentation") or row.get("apresentacao") or "").strip() or None,
        "laboratory": str(row.get("laboratory") or row.get("laboratorio") or "").strip() or None,
        "dcb_name": str(row.get("dcb_name") or row.get("principio_ativo") or "").strip() or None,
        "ean_gtin": _normalize_identifier(row.get("ean_gtin") or row.get("ean") or row.get("gtin")),
        "anvisa_code": _normalize_identifier(row.get("anvisa_code") or row.get("registro_ms") or row.get("registro_anvisa"), valid_lengths=(8, 9, 10, 11, 12, 13)),
        "pmc_price": _normalize_money(row.get("pmc_price") or row.get("pmc")),
        "pf_price": _normalize_money(row.get("pf_price") or row.get("pf")),
        "list_price": _normalize_money(row.get("list_price") or row.get("preco_fabrica") or row.get("preco")),
        "tax_rate": str(row.get("tax_rate") or row.get("icms") or "").strip() or None,
        "source_url": str(row.get("source_url") or row.get("url") or "").strip() or None,
        "source_payload": row,
    }
    normalized["row_fingerprint"] = _cmed_fingerprint(normalized)
    return normalized


def _upsert(session, model, lookup, values):
    instance = session.query(model).filter_by(**lookup).first()
    created = False
    if not instance:
        instance = model(**lookup)
        session.add(instance)
        created = True
    for field_name, value in values.items():
        setattr(instance, field_name, value)
    instance.last_imported_at = _now_naive()
    return created


def import_regulatory_products(session, file_path, replace=False):
    summary = ImportSummary(dataset="regulatory_products", file_path=str(file_path), replaced=replace)
    if replace:
        _replace_dataset(session, RegulatoryProduct)
    for row in _load_rows(file_path):
        summary.rows_read += 1
        normalized = _normalize_regulatory_row(row)
        if not normalized:
            summary.skipped += 1
            continue
        lookup = {
            "source_system": normalized["source_system"],
            "external_id": normalized["external_id"],
        }
        created = _upsert(session, RegulatoryProduct, lookup, normalized)
        if created:
            summary.created += 1
        else:
            summary.updated += 1
    session.commit()
    log_event(LOGGER, 20, "reference_data_import_completed", **summary.__dict__)
    return summary


def import_dcb_aliases(session, file_path, replace=False):
    summary = ImportSummary(dataset="dcb_aliases", file_path=str(file_path), replaced=replace)
    if replace:
        _replace_dataset(session, RegulatoryAlias)
    for row in _load_rows(file_path):
        summary.rows_read += 1
        normalized = _normalize_dcb_alias_row(row)
        if not normalized:
            summary.skipped += 1
            continue
        lookup = {
            "alias_type": normalized["alias_type"],
            "normalized_alias": normalized["normalized_alias"],
        }
        created = _upsert(session, RegulatoryAlias, lookup, normalized)
        if created:
            summary.created += 1
        else:
            summary.updated += 1
    session.commit()
    log_event(LOGGER, 20, "reference_data_import_completed", **summary.__dict__)
    return summary


def import_cmed_prices(session, file_path, replace=False):
    summary = ImportSummary(dataset="cmed_prices", file_path=str(file_path), replaced=replace)
    if replace:
        _replace_dataset(session, CmedPriceEntry)
    for row in _load_rows(file_path):
        summary.rows_read += 1
        normalized = _normalize_cmed_row(row)
        if not normalized:
            summary.skipped += 1
            continue
        lookup = {
            "source_dataset": normalized["source_dataset"],
            "row_fingerprint": normalized["row_fingerprint"],
        }
        created = _upsert(session, CmedPriceEntry, lookup, normalized)
        if created:
            summary.created += 1
        else:
            summary.updated += 1
    session.commit()
    log_event(LOGGER, 20, "reference_data_import_completed", **summary.__dict__)
    return summary


def import_reference_data(session, *, regulatory_file=None, dcb_file=None, cmed_file=None, replace=False):
    summaries = []
    if regulatory_file and Path(regulatory_file).exists():
        summaries.append(import_regulatory_products(session, regulatory_file, replace=replace))
    elif regulatory_file:
        log_event(LOGGER, 30, "reference_data_file_missing", dataset="regulatory_products", file_path=str(regulatory_file))
    if dcb_file and Path(dcb_file).exists():
        summaries.append(import_dcb_aliases(session, dcb_file, replace=replace))
    elif dcb_file:
        log_event(LOGGER, 30, "reference_data_file_missing", dataset="dcb_aliases", file_path=str(dcb_file))
    if cmed_file and Path(cmed_file).exists():
        summaries.append(import_cmed_prices(session, cmed_file, replace=replace))
    elif cmed_file:
        log_event(LOGGER, 30, "reference_data_file_missing", dataset="cmed_prices", file_path=str(cmed_file))
    return summaries
