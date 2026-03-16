from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    source_type: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    articles: Mapped[list[ArticleORM]] = relationship(back_populates="source_rel")
    scrape_runs: Mapped[list[ScrapeRun]] = relationship(back_populates="source_rel")


class ArticleORM(Base):
    __tablename__ = "articles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    url_hash: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    content_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    word_count: Mapped[int] = mapped_column(Integer, default=0)

    source_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sources.id"), nullable=True
    )
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    language: Mapped[str] = mapped_column(Text, default="en")

    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    scraped_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    # JSON columns (TEXT in SQLite, JSONB in PG via migration)
    tickers: Mapped[Optional[str]] = mapped_column(Text, default="[]")
    companies: Mapped[Optional[str]] = mapped_column(Text, default="[]")
    sectors: Mapped[Optional[str]] = mapped_column(Text, default="[]")

    sentiment_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quality_score: Mapped[float] = mapped_column(Float, default=0.0)
    quality_flags: Mapped[Optional[str]] = mapped_column(Text, default="[]")

    is_paywall: Mapped[bool] = mapped_column(Boolean, default=False)
    is_duplicate: Mapped[bool] = mapped_column(Boolean, default=False)
    is_near_duplicate: Mapped[bool] = mapped_column(Boolean, default=False)
    is_metadata_only: Mapped[bool] = mapped_column(Boolean, default=False)
    simhash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    external_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    raw_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    source_rel: Mapped[Optional[Source]] = relationship(back_populates="articles")

    __table_args__ = (
        UniqueConstraint("source_name", "external_id", name="uq_source_external_id"),
        Index("ix_articles_published_at", "published_at"),
        Index("ix_articles_source_name", "source_name"),
        Index("ix_articles_url_hash", "url_hash"),
        Index("ix_articles_content_hash", "content_hash"),
        Index("ix_articles_simhash", "simhash"),
    )

    # Helpers for JSON columns
    def get_tickers(self) -> list[str]:
        return json.loads(self.tickers or "[]")

    def get_companies(self) -> list[str]:
        return json.loads(self.companies or "[]")

    def get_sectors(self) -> list[str]:
        return json.loads(self.sectors or "[]")

    def get_quality_flags(self) -> list[str]:
        return json.loads(self.quality_flags or "[]")


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("sources.id"), nullable=True
    )
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    run_type: Mapped[str] = mapped_column(Text, nullable=False)  # realtime|backfill|manual
    status: Mapped[str] = mapped_column(Text, default="running")  # running|completed|failed

    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    articles_found: Mapped[int] = mapped_column(Integer, default=0)
    articles_stored: Mapped[int] = mapped_column(Integer, default=0)
    articles_duped: Mapped[int] = mapped_column(Integer, default=0)
    articles_failed: Mapped[int] = mapped_column(Integer, default=0)

    # Stores date_cursor + url_cursor for resumable backfills
    metadata_json: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    source_rel: Mapped[Optional[Source]] = relationship(back_populates="scrape_runs")

    def get_metadata(self) -> dict:
        return json.loads(self.metadata_json or "{}")

    def set_metadata(self, data: dict) -> None:
        self.metadata_json = json.dumps(data)


class QualityLog(Base):
    __tablename__ = "quality_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    article_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("articles.id"), nullable=True
    )
    url: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str] = mapped_column(Text, nullable=False)
    check_name: Mapped[str] = mapped_column(Text, nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    logged_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
