from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, field_validator


class RawArticle(BaseModel):
    """Intermediate model — output of a fetcher, before extraction/enrichment."""

    url: str
    title: str
    summary: Optional[str] = None
    published_at: datetime
    source_name: str
    author: Optional[str] = None
    external_id: Optional[str] = None
    raw_json: Optional[str] = None


class Article(BaseModel):
    """Canonical article model — fully enriched, ready for DB write."""

    url: str
    url_hash: str
    content_hash: Optional[str] = None
    title: str
    body: str
    summary: Optional[str] = None
    word_count: int = 0
    source_name: str
    source_id: Optional[int] = None
    author: Optional[str] = None
    language: str = "en"
    published_at: datetime
    scraped_at: datetime
    tickers: list[str] = []
    companies: list[str] = []
    sectors: list[str] = []
    sentiment_score: Optional[float] = None
    quality_score: float = 0.0
    quality_flags: list[str] = []
    is_paywall: bool = False
    is_duplicate: bool = False
    is_near_duplicate: bool = False
    is_metadata_only: bool = False
    simhash: Optional[str] = None
    external_id: Optional[str] = None
    raw_json: Optional[str] = None

    @field_validator("word_count", mode="before")
    @classmethod
    def compute_word_count(cls, v: int, info) -> int:
        if v == 0 and "body" in (info.data or {}):
            return len(info.data["body"].split())
        return v
