import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class MeliConfig:
    """
    Configuração da API - Single Source of Truth.

    Conceito (12-Factor App, Fator III): toda config vem de env vars.
    Isso permite trocar de ambiente (dev/staging/prod) sem alterar código.
    """
    base_url: str = "https://api.mercadolibre.com"
    site_id: str = "MLB"
    client_id: Optional[str] = os.getenv("MELI_CLIENT_ID")
    client_secret: Optional[str] = os.getenv("MELI_CLIENT_SECRET")
    access_token: Optional[str] = os.getenv("MELI_ACCESS_TOKEN")
    refresh_token: Optional[str] = os.getenv("MELI_REFRESH_TOKEN")
    requests_per_second: float = 0.8
    max_retries: int = 3
    retry_delay: int = 5
    page_size: int = 50
    max_results_per_search: int = 1000


@dataclass
class StorageConfig:
    """
    Medallion Architecture:
    - Raw (Bronze): JSONs imutáveis, append-only
    - Refined (Silver/Gold): SQL Server, tipado e indexado
    """
    raw_path: str = os.getenv("RAW_STORAGE_PATH", "./data/raw")
    db_driver: str = os.getenv("DB_DRIVER", "ODBC Driver 17 for SQL Server")
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_name: str = os.getenv("DB_NAME", "meli_intelligence")
    db_user: str = os.getenv("DB_USER", "sa")
    db_password: str = os.getenv("DB_PASSWORD", "")

    @property
    def connection_string(self) -> str:
        return (
            f"DRIVER={{{self.db_driver}}};"
            f"SERVER={self.db_host};"
            f"DATABASE={self.db_name};"
            f"UID={self.db_user};"
            f"PWD={self.db_password};"
            f"TrustServerCertificate=yes;"
        )


@dataclass
class AlertConfig:
    telegram_bot_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    telegram_chat_id: str = os.getenv("TELEGRAM_CHAT_ID", "")
    price_change_threshold: float = 10.0
    new_competitor_alert: bool = True