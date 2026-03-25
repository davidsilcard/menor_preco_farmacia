import argparse

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from src.core.logging import get_logger, log_event
from src.models.base import Base, Pharmacy, engine

LOGGER = get_logger(__name__)

ADDITIVE_SCHEMA_PATCHES = {
    "catalog_requests": {
        "resolution_source": "ALTER TABLE catalog_requests ADD COLUMN resolution_source VARCHAR",
        "last_requested_by_tool": "ALTER TABLE catalog_requests ADD COLUMN last_requested_by_tool VARCHAR",
    },
    "search_jobs": {
        "request_count": "ALTER TABLE search_jobs ADD COLUMN request_count INTEGER",
        "position_hint": "ALTER TABLE search_jobs ADD COLUMN position_hint INTEGER",
        "eta_seconds": "ALTER TABLE search_jobs ADD COLUMN eta_seconds INTEGER",
        "result_payload": "ALTER TABLE search_jobs ADD COLUMN result_payload JSON",
        "error_message": "ALTER TABLE search_jobs ADD COLUMN error_message VARCHAR",
        "started_at": "ALTER TABLE search_jobs ADD COLUMN started_at TIMESTAMP",
        "finished_at": "ALTER TABLE search_jobs ADD COLUMN finished_at TIMESTAMP",
        "catalog_request_id": "ALTER TABLE search_jobs ADD COLUMN catalog_request_id INTEGER",
    },
    "tracked_items_by_cep": {
        "status": "ALTER TABLE tracked_items_by_cep ADD COLUMN status VARCHAR",
        "request_count_total": "ALTER TABLE tracked_items_by_cep ADD COLUMN request_count_total INTEGER",
        "scrape_priority": "ALTER TABLE tracked_items_by_cep ADD COLUMN scrape_priority DOUBLE PRECISION",
        "first_requested_at": "ALTER TABLE tracked_items_by_cep ADD COLUMN first_requested_at TIMESTAMP",
        "last_requested_at": "ALTER TABLE tracked_items_by_cep ADD COLUMN last_requested_at TIMESTAMP",
        "last_scraped_at": "ALTER TABLE tracked_items_by_cep ADD COLUMN last_scraped_at TIMESTAMP",
        "last_requested_by_tool": "ALTER TABLE tracked_items_by_cep ADD COLUMN last_requested_by_tool VARCHAR",
        "source_kind": "ALTER TABLE tracked_items_by_cep ADD COLUMN source_kind VARCHAR",
        "last_match_confidence": "ALTER TABLE tracked_items_by_cep ADD COLUMN last_match_confidence DOUBLE PRECISION",
    },
    "source_products": {
        "source_url": "ALTER TABLE source_products ADD COLUMN source_url VARCHAR",
        "ean_gtin": "ALTER TABLE source_products ADD COLUMN ean_gtin VARCHAR",
        "anvisa_code": "ALTER TABLE source_products ADD COLUMN anvisa_code VARCHAR",
        "source_metadata": "ALTER TABLE source_products ADD COLUMN source_metadata JSON",
        "is_active": "ALTER TABLE source_products ADD COLUMN is_active BOOLEAN",
    },
    "price_snapshots": {
        "list_price": "ALTER TABLE price_snapshots ADD COLUMN list_price DOUBLE PRECISION",
        "availability": "ALTER TABLE price_snapshots ADD COLUMN availability VARCHAR",
        "cep": "ALTER TABLE price_snapshots ADD COLUMN cep VARCHAR",
        "source_url": "ALTER TABLE price_snapshots ADD COLUMN source_url VARCHAR",
        "promotion_text": "ALTER TABLE price_snapshots ADD COLUMN promotion_text VARCHAR",
    },
    "scrape_runs": {
        "trigger_type": "ALTER TABLE scrape_runs ADD COLUMN trigger_type VARCHAR",
        "search_terms": "ALTER TABLE scrape_runs ADD COLUMN search_terms JSON",
        "products_seen": "ALTER TABLE scrape_runs ADD COLUMN products_seen INTEGER",
        "products_saved": "ALTER TABLE scrape_runs ADD COLUMN products_saved INTEGER",
        "error_count": "ALTER TABLE scrape_runs ADD COLUMN error_count INTEGER",
        "error_message": "ALTER TABLE scrape_runs ADD COLUMN error_message VARCHAR",
    },
    "operation_jobs": {
        "request_count": "ALTER TABLE operation_jobs ADD COLUMN request_count INTEGER",
        "payload": "ALTER TABLE operation_jobs ADD COLUMN payload JSON",
        "payload_fingerprint": "ALTER TABLE operation_jobs ADD COLUMN payload_fingerprint VARCHAR",
        "result_payload": "ALTER TABLE operation_jobs ADD COLUMN result_payload JSON",
        "error_message": "ALTER TABLE operation_jobs ADD COLUMN error_message VARCHAR",
        "started_at": "ALTER TABLE operation_jobs ADD COLUMN started_at TIMESTAMP",
        "finished_at": "ALTER TABLE operation_jobs ADD COLUMN finished_at TIMESTAMP",
    },
}


def _schema_patch_statements(existing_tables: set[str], existing_columns_by_table: dict[str, set[str]]):
    statements = []
    for table_name, column_patches in ADDITIVE_SCHEMA_PATCHES.items():
        if table_name not in existing_tables:
            continue
        existing_columns = existing_columns_by_table.get(table_name, set())
        for column_name, sql in column_patches.items():
            if column_name not in existing_columns:
                statements.append(sql)
    return statements


def apply_additive_schema_updates():
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    existing_columns_by_table = {
        table_name: {column["name"] for column in inspector.get_columns(table_name)}
        for table_name in existing_tables
    }
    statements = _schema_patch_statements(existing_tables, existing_columns_by_table)
    if not statements:
        return 0

    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))

    log_event(LOGGER, 20, "database_schema_patched", statements_applied=len(statements))
    return len(statements)


def reset_db():
    log_event(LOGGER, 20, "database_reset_started")
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))


def init_db():
    log_event(LOGGER, 20, "database_init_started")
    Base.metadata.create_all(bind=engine)
    apply_additive_schema_updates()

    initial_pharmacies = [
        {"name": "Panvel", "slug": "panvel", "website": "https://www.panvel.com"},
        {"name": "FarmaSesi", "slug": "farmasesi", "website": "https://www.farmasesi.com.br"},
        {"name": "Sao Joao", "slug": "sao-joao", "website": "https://www.saojoaofarmacias.com.br"},
        {"name": "Farmacia Jaragua", "slug": "farmacia-jaragua", "website": "https://farmaciajaragua.com.br"},
        {"name": "Drogasil", "slug": "drogasil", "website": "https://www.drogasil.com.br"},
        {"name": "Droga Raia", "slug": "droga-raia", "website": "https://www.drogaraia.com.br"},
        {"name": "Drogaria Sao Paulo", "slug": "drogaria-sao-paulo", "website": "https://www.drogariasaopaulo.com.br"},
        {"name": "Drogaria Catarinense", "slug": "drogaria-catarinense", "website": "https://www.drogariacatarinense.com.br"},
        {"name": "Preco Popular", "slug": "preco-popular", "website": "https://www.precopopular.com.br"},
    ]

    with Session(engine) as session:
        for pharmacy_data in initial_pharmacies:
            pharmacy = session.query(Pharmacy).filter_by(slug=pharmacy_data["slug"]).first()
            if pharmacy:
                pharmacy.name = pharmacy_data["name"]
                pharmacy.website = pharmacy_data["website"]
            else:
                session.add(Pharmacy(**pharmacy_data))

        session.commit()
        log_event(LOGGER, 20, "database_init_completed", pharmacies_seeded=len(initial_pharmacies))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inicializa o schema do banco de dados.")
    parser.add_argument("--reset", action="store_true", help="Remove o schema publico antes de recriar as tabelas.")
    args = parser.parse_args()

    if args.reset:
        reset_db()
    init_db()
