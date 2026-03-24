import argparse

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.core.logging import get_logger, log_event
from src.models.base import Base, Pharmacy, engine

LOGGER = get_logger(__name__)


def reset_db():
    log_event(LOGGER, 20, "database_reset_started")
    with engine.begin() as connection:
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))


def init_db():
    log_event(LOGGER, 20, "database_init_started")
    Base.metadata.create_all(bind=engine)

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
