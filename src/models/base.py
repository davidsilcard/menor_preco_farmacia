from datetime import datetime, timedelta

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


engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def delete_old_prices():
    """Remove snapshots capturados ha mais de 3 meses."""
    three_months_ago = datetime.utcnow() - timedelta(days=90)
    with SessionLocal() as session:
        stmt = delete(PriceSnapshot).where(PriceSnapshot.captured_at < three_months_ago)
        result = session.execute(stmt)
        session.commit()
        print(f"Limpeza concluida. {result.rowcount} snapshots antigos foram removidos.")
