from pydantic_settings import BaseSettings, SettingsConfigDict
import os
from dotenv import load_dotenv

# Carrega o .env da raiz
load_dotenv()

class Settings(BaseSettings):
    # Definindo os campos com base no seu .env real
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "admin")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
    DB_HOST: str = os.getenv("DB_HOST", "192.168.25.203")
    DB_PORT: str = os.getenv("DB_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "precos-farmacia")
    PORT: int = int(os.getenv("PORT", "8000"))
    
    CEP: str = os.getenv("CEP", "89254300")
    PANVEL_SEARCH_TERMS: str = os.getenv("PANVEL_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    FARMASESI_SEARCH_TERMS: str = os.getenv("FARMASESI_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    SAO_JOAO_SEARCH_TERMS: str = os.getenv("SAO_JOAO_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    FARMACIA_JARAGUA_SEARCH_TERMS: str = os.getenv("FARMACIA_JARAGUA_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    DROGASIL_SEARCH_TERMS: str = os.getenv("DROGASIL_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    CATARINENSE_SEARCH_TERMS: str = os.getenv("CATARINENSE_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    PRECO_POPULAR_SEARCH_TERMS: str = os.getenv("PRECO_POPULAR_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    DROGA_RAIA_SEARCH_TERMS: str = os.getenv("DROGA_RAIA_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno")
    DROGARIA_SAO_PAULO_SEARCH_TERMS: str = os.getenv(
        "DROGARIA_SAO_PAULO_SEARCH_TERMS", "dipirona,paracetamol,ibuprofeno"
    )
    ON_DEMAND_ENABLE_BROWSER_SCRAPERS: bool = os.getenv("ON_DEMAND_ENABLE_BROWSER_SCRAPERS", "false").lower() == "true"
    SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS: bool = os.getenv(
        "SCHEDULED_COLLECTION_ENABLE_BROWSER_SCRAPERS", "false"
    ).lower() == "true"
    SCHEDULED_COLLECTION_MAX_ITEMS_PER_CEP: int = int(os.getenv("SCHEDULED_COLLECTION_MAX_ITEMS_PER_CEP", "50"))

    @property
    def DATABASE_URL(self) -> str:
        # Monta a URL dinamicamente
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.POSTGRES_DB}"

settings = Settings()
print(f"DEBUG: Conectando em {settings.DB_HOST} no banco {settings.POSTGRES_DB}")
