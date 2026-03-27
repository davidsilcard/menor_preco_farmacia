from datetime import UTC, datetime

from sqlalchemy.orm import Session

from src.models.base import Pharmacy, PharmacyRegionCoverage
from src.services.catalog_queries import normalize_cep


def pharmacy_region_coverage_payload(coverage: PharmacyRegionCoverage, *, requested_cep: str | None = None):
    return {
        "pharmacy_region_coverage_id": coverage.id,
        "pharmacy_id": coverage.pharmacy_id,
        "pharmacy_slug": coverage.pharmacy.slug if getattr(coverage, "pharmacy", None) else None,
        "pharmacy_name": coverage.pharmacy.name if getattr(coverage, "pharmacy", None) else None,
        "city": coverage.city,
        "state": coverage.state,
        "cep_start": coverage.cep_start,
        "cep_end": coverage.cep_end,
        "status": coverage.status,
        "confidence": coverage.confidence,
        "verification_source": coverage.verification_source,
        "last_verified_at": coverage.last_verified_at,
        "notes": coverage.notes,
        "requested_cep": requested_cep,
    }


def coverage_entries_for_cep(db: Session, cep: str | None):
    normalized_cep = normalize_cep(cep)
    if not normalized_cep:
        return []

    try:
        entries = (
            db.query(PharmacyRegionCoverage)
            .join(Pharmacy, Pharmacy.id == PharmacyRegionCoverage.pharmacy_id)
            .order_by(Pharmacy.slug.asc(), PharmacyRegionCoverage.city.asc(), PharmacyRegionCoverage.cep_start.asc())
            .all()
        )
    except (AttributeError, TypeError):
        entries = db.query(PharmacyRegionCoverage).all()
    return [
        entry
        for entry in entries
        if (entry.cep_start or "") <= normalized_cep <= (entry.cep_end or "")
    ]


def scraper_coverage_decision(db: Session, pharmacy_slug: str, cep: str | None):
    normalized_cep = normalize_cep(cep)
    if not normalized_cep:
        return {
            "allowed": True,
            "status": "unknown",
            "confidence": None,
            "reason": "missing_cep",
            "coverage": None,
        }

    coverage = next(
        (
            entry
            for entry in coverage_entries_for_cep(db, normalized_cep)
            if getattr(getattr(entry, "pharmacy", None), "slug", None) == pharmacy_slug
        ),
        None,
    )
    if not coverage:
        return {
            "allowed": True,
            "status": "unknown",
            "confidence": None,
            "reason": "no_declared_pharmacy_coverage",
            "coverage": None,
        }

    status = (coverage.status or "unknown").strip().lower()
    allowed = status not in {"unsupported", "inactive", "disabled"}
    return {
        "allowed": allowed,
        "status": status,
        "confidence": coverage.confidence,
        "reason": "declared_supported" if allowed else "declared_unsupported",
        "coverage": coverage,
    }


def seeded_pharmacy_region_coverages():
    now = datetime.now(UTC).replace(tzinfo=None)
    observed_source = "observed_runtime_validation"
    official_source = "official_store_list"
    return [
        {
            "pharmacy_slug": "drogaria-sao-paulo",
            "city": "Jaragua do Sul",
            "state": "SC",
            "cep_start": "89250000",
            "cep_end": "89269999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na operacao inicial.",
        },
        {
            "pharmacy_slug": "drogaria-sao-paulo",
            "city": "Guaramirim",
            "state": "SC",
            "cep_start": "89270000",
            "cep_end": "89274999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "drogaria-sao-paulo",
            "city": "Schroeder",
            "state": "SC",
            "cep_start": "89275000",
            "cep_end": "89279999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "drogaria-catarinense",
            "city": "Jaragua do Sul",
            "state": "SC",
            "cep_start": "89250000",
            "cep_end": "89269999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na operacao inicial.",
        },
        {
            "pharmacy_slug": "drogaria-catarinense",
            "city": "Guaramirim",
            "state": "SC",
            "cep_start": "89270000",
            "cep_end": "89274999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "drogaria-catarinense",
            "city": "Schroeder",
            "state": "SC",
            "cep_start": "89275000",
            "cep_end": "89279999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "preco-popular",
            "city": "Jaragua do Sul",
            "state": "SC",
            "cep_start": "89250000",
            "cep_end": "89269999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na operacao inicial.",
        },
        {
            "pharmacy_slug": "preco-popular",
            "city": "Guaramirim",
            "state": "SC",
            "cep_start": "89270000",
            "cep_end": "89274999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "preco-popular",
            "city": "Schroeder",
            "state": "SC",
            "cep_start": "89275000",
            "cep_end": "89279999",
            "status": "active",
            "confidence": "observed",
            "verification_source": observed_source,
            "last_verified_at": now,
            "notes": "Rede com oferta real confirmada na expansao.",
        },
        {
            "pharmacy_slug": "farmasesi",
            "city": "Jaragua do Sul",
            "state": "SC",
            "cep_start": "89250000",
            "cep_end": "89269999",
            "status": "active",
            "confidence": "official",
            "verification_source": official_source,
            "last_verified_at": now,
            "notes": "Cobertura oficial observada na lista publica de lojas.",
        },
        {
            "pharmacy_slug": "farmasesi",
            "city": "Guaramirim",
            "state": "SC",
            "cep_start": "89270000",
            "cep_end": "89274999",
            "status": "unsupported",
            "confidence": "official",
            "verification_source": official_source,
            "last_verified_at": now,
            "notes": "Sem cobertura oficial confirmada na lista publica de lojas.",
        },
        {
            "pharmacy_slug": "farmasesi",
            "city": "Schroeder",
            "state": "SC",
            "cep_start": "89275000",
            "cep_end": "89279999",
            "status": "unsupported",
            "confidence": "official",
            "verification_source": official_source,
            "last_verified_at": now,
            "notes": "Sem cobertura oficial confirmada na lista publica de lojas.",
        },
    ]
