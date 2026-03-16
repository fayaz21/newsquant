from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, field_validator


class RawArticle(BaseModel):
    """Intermediate model — output of a fetcher, before extraction/enrichment."""

    url: str
    title: str
    summary: str | None = None
    published_at: datetime
    source_name: str
    author: str | None = None
    external_id: str | None = None
    raw_json: str | None = None


class Article(BaseModel):
    """Canonical article model — fully enriched, ready for DB write."""

    url: str
    url_hash: str
    content_hash: str | None = None
    title: str
    body: str
    summary: str | None = None
    word_count: int = 0
    source_name: str
    source_id: int | None = None
    author: str | None = None
    language: str = "en"
    published_at: datetime
    scraped_at: datetime
    tickers: list[str] = []
    companies: list[str] = []
    sectors: list[str] = []
    sentiment_score: float | None = None
    quality_score: float = 0.0
    quality_flags: list[str] = []
    is_paywall: bool = False
    is_duplicate: bool = False
    is_near_duplicate: bool = False
    is_metadata_only: bool = False
    simhash: str | None = None
    external_id: str | None = None
    raw_json: str | None = None

    @field_validator("word_count", mode="before")
    @classmethod
    def compute_word_count(cls, v: int, info) -> int:
        if v == 0 and "body" in (info.data or {}):
            return len(info.data["body"].split())
        return v
