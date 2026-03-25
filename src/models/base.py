from datetime import UTC, datetime, timedelta

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    JSON,
    String,
    UniqueConstraint,
    create_engine,
    delete,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from src.core.config import settings

Base = declarative_base()


class Pharmacy(Base):
    __tablename__ = "pharmacies"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    slug = Column(String, unique=True, nullable=False)
    website = Column(String)

    source_products = relationship("SourceProduct", back_populates="pharmacy")
    scrape_runs = relationship("ScrapeRun", back_populates="pharmacy")


class CanonicalProduct(Base):
    __tablename__ = "canonical_products"

    id = Column(Integer, primary_key=True)
    canonical_name = Column(String, nullable=False)
    normalized_name = Column(String, nullable=False, index=True)
    brand = Column(String)
    manufacturer = Column(String)
    active_ingredient = Column(String)
    dosage = Column(String)
    presentation = Column(String)
    pack_size = Column(String)
    ean_gtin = Column(String, unique=True)
    anvisa_code = Column(String, unique=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    matches = relationship("ProductMatch", back_populates="canonical_product")


class RegulatoryProduct(Base):
    __tablename__ = "regulatory_products"
    __table_args__ = (
        UniqueConstraint("source_system", "external_id", name="uq_regulatory_products_source_external"),
    )

    id = Column(Integer, primary_key=True)
    source_system = Column(String, nullable=False, default="anvisa", index=True)
    external_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    normalized_product_name = Column(String, nullable=False, index=True)
    dcb_name = Column(String, index=True)
    active_ingredient = Column(String)
    concentration = Column(String)
    dosage = Column(String)
    dosage_form = Column(String)
    presentation = Column(String)
    route = Column(String)
    manufacturer = Column(String)
    registration_holder = Column(String)
    ean_gtin = Column(String, index=True)
    anvisa_code = Column(String, index=True)
    source_url = Column(String)
    source_payload = Column(JSON)
    last_imported_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class RegulatoryAlias(Base):
    __tablename__ = "regulatory_aliases"
    __table_args__ = (
        UniqueConstraint("alias_type", "normalized_alias", name="uq_regulatory_aliases_type_alias"),
    )

    id = Column(Integer, primary_key=True)
    alias_type = Column(String, nullable=False, default="dcb", index=True)
    dcb_name = Column(String, nullable=False, index=True)
    alias = Column(String, nullable=False)
    normalized_alias = Column(String, nullable=False, index=True)
    source_system = Column(String, nullable=False, default="anvisa")
    source_payload = Column(JSON)
    last_imported_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class CmedPriceEntry(Base):
    __tablename__ = "cmed_price_entries"
    __table_args__ = (
        UniqueConstraint("source_dataset", "row_fingerprint", name="uq_cmed_price_entries_dataset_fingerprint"),
    )

    id = Column(Integer, primary_key=True)
    source_dataset = Column(String, nullable=False, default="cmed", index=True)
    row_fingerprint = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    normalized_product_name = Column(String, nullable=False, index=True)
    presentation = Column(String)
    laboratory = Column(String)
    dcb_name = Column(String, index=True)
    ean_gtin = Column(String, index=True)
    anvisa_code = Column(String, index=True)
    pmc_price = Column(Float)
    pf_price = Column(Float)
    list_price = Column(Float)
    tax_rate = Column(String)
    source_url = Column(String)
    source_payload = Column(JSON)
    last_imported_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class SourceProduct(Base):
    __tablename__ = "source_products"
    __table_args__ = (
        UniqueConstraint("pharmacy_id", "source_sku", name="uq_source_products_pharmacy_sku"),
    )

    id = Column(Integer, primary_key=True)
    pharmacy_id = Column(Integer, ForeignKey("pharmacies.id"), nullable=False, index=True)
    source_sku = Column(String, nullable=False)
    source_url = Column(String)
    raw_name = Column(String, nullable=False)
    normalized_name = Column(String, nullable=False, index=True)
    brand = Column(String)
    manufacturer = Column(String)
    active_ingredient = Column(String)
    dosage = Column(String)
    presentation = Column(String)
    pack_size = Column(String)
    ean_gtin = Column(String, index=True)
    anvisa_code = Column(String, index=True)
    source_metadata = Column(JSON)
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    pharmacy = relationship("Pharmacy", back_populates="source_products")
    prices = relationship("PriceSnapshot", back_populates="source_product")
    match = relationship("ProductMatch", back_populates="source_product", uselist=False)


class ProductMatch(Base):
    __tablename__ = "product_matches"
    __table_args__ = (
        UniqueConstraint("source_product_id", name="uq_product_matches_source_product"),
    )

    id = Column(Integer, primary_key=True)
    source_product_id = Column(Integer, ForeignKey("source_products.id"), nullable=False)
    canonical_product_id = Column(Integer, ForeignKey("canonical_products.id"), nullable=False)
    match_type = Column(String, nullable=False, default="manual_review")
    confidence = Column(Float, nullable=False, default=0.0)
    review_status = Column(String, nullable=False, default="pending")
    review_notes = Column(String)
    matched_at = Column(DateTime, default=datetime.utcnow, nullable=False)

    source_product = relationship("SourceProduct", back_populates="match")
    canonical_product = relationship("CanonicalProduct", back_populates="matches")


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    id = Column(Integer, primary_key=True)
    source_product_id = Column(Integer, ForeignKey("source_products.id"), nullable=False, index=True)
    scrape_run_id = Column(Integer, ForeignKey("scrape_runs.id"), nullable=False, index=True)
    price = Column(Float, nullable=False)
    list_price = Column(Float)
    availability = Column(String, default="unknown", nullable=False)
    captured_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    cep = Column(String)
    source_url = Column(String)
    promotion_text = Column(String)

    source_product = relationship("SourceProduct", back_populates="prices")
    scrape_run = relationship("ScrapeRun", back_populates="price_snapshots")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id = Column(Integer, primary_key=True)
    pharmacy_id = Column(Integer, ForeignKey("pharmacies.id"), nullable=False, index=True)
    cep = Column(String, nullable=False)
    trigger_type = Column(String, nullable=False, default="scheduled")
    status = Column(String, nullable=False, default="running")
    search_terms = Column(JSON)
    products_seen = Column(Integer, nullable=False, default=0)
    products_saved = Column(Integer, nullable=False, default=0)
    error_count = Column(Integer, nullable=False, default=0)
    error_message = Column(String)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at = Column(DateTime)

    pharmacy = relationship("Pharmacy", back_populates="scrape_runs")
    price_snapshots = relationship("PriceSnapshot", back_populates="scrape_run")


class CatalogRequest(Base):
    __tablename__ = "catalog_requests"
    __table_args__ = (
        UniqueConstraint("normalized_query", "cep", name="uq_catalog_requests_query_cep"),
    )

    id = Column(Integer, primary_key=True)
    query = Column(String, nullable=False)
    normalized_query = Column(String, nullable=False, index=True)
    cep = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default="pending")
    resolution_source = Column(String)
    request_count = Column(Integer, nullable=False, default=1)
    first_requested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_requested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_requested_by_tool = Column(String)


class SearchJob(Base):
    __tablename__ = "search_jobs"

    id = Column(Integer, primary_key=True)
    query = Column(String, nullable=False)
    normalized_query = Column(String, nullable=False, index=True)
    cep = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default="queued")
    requested_by_tool = Column(String, nullable=False)
    request_count = Column(Integer, nullable=False, default=1)
    position_hint = Column(Integer)
    eta_seconds = Column(Integer)
    result_payload = Column(JSON)
    error_message = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    catalog_request_id = Column(Integer, ForeignKey("catalog_requests.id"), index=True)


class OperationJob(Base):
    __tablename__ = "operation_jobs"

    id = Column(Integer, primary_key=True)
    job_type = Column(String, nullable=False, index=True)
    requested_by = Column(String, nullable=False)
    status = Column(String, nullable=False, default="queued", index=True)
    request_count = Column(Integer, nullable=False, default=1)
    payload = Column(JSON)
    payload_fingerprint = Column(String, nullable=False, index=True)
    result_payload = Column(JSON)
    error_message = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    started_at = Column(DateTime)
    finished_at = Column(DateTime)


class TrackedItemByCep(Base):
    __tablename__ = "tracked_items_by_cep"
    __table_args__ = (
        UniqueConstraint("cep", "normalized_query", name="uq_tracked_items_by_cep_query"),
        UniqueConstraint("cep", "canonical_product_id", name="uq_tracked_items_by_cep_canonical"),
    )

    id = Column(Integer, primary_key=True)
    cep = Column(String, nullable=False, index=True)
    query = Column(String, nullable=False)
    normalized_query = Column(String, nullable=False, index=True)
    canonical_product_id = Column(Integer, ForeignKey("canonical_products.id"), index=True)
    status = Column(String, nullable=False, default="active")
    request_count_total = Column(Integer, nullable=False, default=1)
    scrape_priority = Column(Float, nullable=False, default=100.0)
    first_requested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_requested_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    last_scraped_at = Column(DateTime)
    last_requested_by_tool = Column(String)
    source_kind = Column(String)
    last_match_confidence = Column(Float)


engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def delete_old_prices(*, retention_days: int | None = None, session_factory=None):
    """Remove snapshots capturados antes da janela de retencao configurada."""
    retention_days = retention_days or settings.PRICE_RETENTION_DAYS
    session_factory = session_factory or SessionLocal
    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=retention_days)
    with session_factory() as session:
        stmt = delete(PriceSnapshot).where(PriceSnapshot.captured_at < cutoff)
        result = session.execute(stmt)
        session.commit()
        return result.rowcount or 0
