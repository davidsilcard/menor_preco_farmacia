from sqlalchemy import text
from src.models.base import engine, SessionLocal


def test_connection():
    try:
        # Tenta conectar e executar uma query simples
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()
            print("Conexao com o PostgreSQL estabelecida com sucesso.")
            print(f"Versao do banco: {version[0]}")
            return True
    except Exception as e:
        print("Falha na conexao com o banco de dados.")
        print(f"Erro: {e}")
        return False


if __name__ == "__main__":
    test_connection()
