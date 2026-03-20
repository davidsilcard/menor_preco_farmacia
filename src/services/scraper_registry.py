from src.scrapers.drogaria_catarinense import DrogariaCatarinenseScraper
from src.scrapers.drogaria_sao_paulo import DrogariaSaoPauloScraper
from src.scrapers.drogasil import DrogasilScraper
from src.scrapers.droga_raia import DrogaRaiaScraper
from src.scrapers.farmacia_jaragua import FarmaciaJaraguaScraper
from src.scrapers.farmasesi import FarmaSesiScraper
from src.scrapers.panvel import PanvelScraper
from src.scrapers.preco_popular import PrecoPopularScraper
from src.scrapers.sao_joao import SaoJoaoScraper

SCRAPER_REGISTRY = [
    ("drogaria-sao-paulo", "http", DrogariaSaoPauloScraper),
    ("drogaria-catarinense", "http", DrogariaCatarinenseScraper),
    ("preco-popular", "http", PrecoPopularScraper),
    ("farmasesi", "http", FarmaSesiScraper),
    ("farmacia-jaragua", "browser", FarmaciaJaraguaScraper),
    ("sao-joao", "browser", SaoJoaoScraper),
    ("drogasil", "browser", DrogasilScraper),
    ("droga-raia", "browser", DrogaRaiaScraper),
    ("panvel", "browser", PanvelScraper),
]
