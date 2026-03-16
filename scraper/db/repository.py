from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from scraper.models.article import Article

from .models import ArticleORM, ScrapeRun, Source


class ArticleRepository:
    def __init__(self, session: Session):
        self.session = session

    def upsert(self, article: Article) -> tuple[ArticleORM, bool]:
        """Insert article; skip if url_hash exists. Mark duplicate if content_hash exists.
        Returns (orm_obj, was_inserted)."""
        # Check url_hash dedup
        existing = self.session.scalar(
            select(ArticleORM).where(ArticleORM.url_hash == article.url_hash)
        )
        if existing:
            return existing, False

        # Check content_hash dedup (body duplicate from different URL)
        is_duplicate = False
        if article.content_hash:
            dup = self.session.scalar(
                select(ArticleORM).where(
                    ArticleORM.content_hash == article.content_hash,
                    ArticleORM.id.isnot(None),
                )
            )
            if dup:
                is_duplicate = True

        orm = ArticleORM(
            url=article.url,
            url_hash=article.url_hash,
            content_hash=article.content_hash,
            title=article.title,
            body=article.body,
            summary=article.summary,
            word_count=article.word_count,
            source_id=article.source_id,
            source_name=article.source_name,
            author=article.author,
            language=article.language,
            published_at=article.published_at,
            scraped_at=article.scraped_at,
            tickers=json.dumps(article.tickers),
            companies=json.dumps(article.companies),
            sectors=json.dumps(article.sectors),
            sentiment_score=article.sentiment_score,
            quality_score=article.quality_score,
            quality_flags=json.dumps(article.quality_flags),
            is_paywall=article.is_paywall,
            is_duplicate=is_duplicate or article.is_duplicate,
            is_near_duplicate=article.is_near_duplicate,
            is_metadata_only=article.is_metadata_only,
            simhash=article.simhash,
            external_id=article.external_id,
            raw_json=article.raw_json,
        )
        self.session.add(orm)
        self.session.flush()
        return orm, True

    def url_hash_exists(self, url_hash: str) -> bool:
        return bool(
            self.session.scalar(select(ArticleORM.id).where(ArticleORM.url_hash == url_hash))
        )

    def external_id_exists(self, source_name: str, external_id: str) -> bool:
        if not external_id:
            return False
        return bool(
            self.session.scalar(
                select(ArticleORM.id).where(
                    ArticleORM.source_name == source_name,
                    ArticleORM.external_id == external_id,
                )
            )
        )

    def content_hash_exists(self, content_hash: str) -> bool:
        return bool(
            self.session.scalar(
                select(ArticleORM.id).where(ArticleORM.content_hash == content_hash)
            )
        )

    def recent_simhashes(self, hours: int = 48, limit: int = 5000) -> list[str]:
        """Return simhash values of articles published in the last N hours.
        Used for near-duplicate detection window."""
        from datetime import timedelta, timezone

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        rows = self.session.scalars(
            select(ArticleORM.simhash)
            .where(
                ArticleORM.published_at >= cutoff,
                ArticleORM.simhash.isnot(None),
            )
            .limit(limit)
        )
        return [r for r in rows if r]

    def query_articles(
        self,
        ticker: str | None = None,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        source_name: str | None = None,
        min_quality: float | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ArticleORM]:
        stmt = select(ArticleORM).order_by(ArticleORM.published_at.desc())
        if from_dt:
            stmt = stmt.where(ArticleORM.published_at >= from_dt)
        if to_dt:
            stmt = stmt.where(ArticleORM.published_at <= to_dt)
        if source_name:
            stmt = stmt.where(ArticleORM.source_name == source_name)
        if min_quality is not None:
            stmt = stmt.where(ArticleORM.quality_score >= min_quality)
        if ticker:
            # JSON contains search — works in both SQLite and PG
            stmt = stmt.where(ArticleORM.tickers.contains(f'"{ticker}"'))
        stmt = stmt.limit(limit).offset(offset)
        return list(self.session.scalars(stmt))

    def stats(self) -> dict:
        total = self.session.scalar(select(func.count()).select_from(ArticleORM)) or 0
        dupes = (
            self.session.scalar(
                select(func.count()).select_from(ArticleORM).where(ArticleORM.is_duplicate)
            )
            or 0
        )
        near_dupes = (
            self.session.scalar(
                select(func.count()).select_from(ArticleORM).where(ArticleORM.is_near_duplicate)
            )
            or 0
        )
        paywalled = (
            self.session.scalar(
                select(func.count()).select_from(ArticleORM).where(ArticleORM.is_paywall)
            )
            or 0
        )
        metadata_only = (
            self.session.scalar(
                select(func.count()).select_from(ArticleORM).where(ArticleORM.is_metadata_only)
            )
            or 0
        )
        full_text = total - metadata_only
        avg_quality = self.session.scalar(select(func.avg(ArticleORM.quality_score))) or 0.0
        high_quality = (
            self.session.scalar(
                select(func.count()).select_from(ArticleORM).where(ArticleORM.quality_score >= 0.8)
            )
            or 0
        )
        return {
            "total_articles": total,
            "full_text": full_text,
            "metadata_only": metadata_only,
            "exact_duplicates": dupes,
            "near_duplicates": near_dupes,
            "paywalled": paywalled,
            "high_quality (>=0.8)": high_quality,
            "avg_quality_score": round(float(avg_quality), 3),
        }


class SourceRepository:
    def __init__(self, session: Session):
        self.session = session

    def get_or_create(self, name: str, source_type: str) -> Source:
        source = self.session.scalar(select(Source).where(Source.name == name))
        if not source:
            source = Source(name=name, source_type=source_type)
            self.session.add(source)
            self.session.flush()
        return source


class ScrapeRunRepository:
    def __init__(self, session: Session):
        self.session = session

    def create(self, source_name: str, run_type: str, source_id: int | None = None) -> ScrapeRun:
        run = ScrapeRun(source_name=source_name, run_type=run_type, source_id=source_id)
        self.session.add(run)
        self.session.flush()
        return run

    def finish(
        self,
        run: ScrapeRun,
        status: str,
        found: int,
        stored: int,
        duped: int,
        failed: int,
        error: str | None = None,
    ) -> None:
        run.status = status
        run.ended_at = datetime.now(timezone.utc)
        run.articles_found = found
        run.articles_stored = stored
        run.articles_duped = duped
        run.articles_failed = failed
        run.error_message = error
        self.session.flush()

    def recent(self, source_name: str | None = None, limit: int = 20) -> list[ScrapeRun]:
        stmt = select(ScrapeRun).order_by(ScrapeRun.started_at.desc()).limit(limit)
        if source_name:
            stmt = stmt.where(ScrapeRun.source_name == source_name)
        return list(self.session.scalars(stmt))

    def get_last_backfill(self, source_name: str) -> ScrapeRun | None:
        return self.session.scalar(
            select(ScrapeRun)
            .where(ScrapeRun.source_name == source_name, ScrapeRun.run_type == "backfill")
            .order_by(ScrapeRun.started_at.desc())
        )
