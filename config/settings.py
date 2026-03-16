from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    database_url: str = "sqlite:///./financial_news.db"

    # API Keys
    newsapi_key: str = ""
    finnhub_api_key: str = ""

    # HTTP tuning
    default_rate_limit_rps: float = 2.0
    request_timeout_seconds: int = 30
    max_retries: int = 3

    # Backfill
    backfill_workers: int = 4
    batch_size: int = 10_000

    # Quality
    min_word_count: int = 150
    language_confidence_threshold: float = 0.95

    # Logging
    log_level: str = "INFO"


settings = Settings()
