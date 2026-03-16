from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from scraper.db import (
    ArticleRepository,
    ScrapeRunRepository,
    SourceRepository,
    get_session,
)
from scraper.enrichment import (
    compute_simhash,
    content_hash,
    extract_tickers,
    is_paywalled_domain,
    is_scraper_blocked,
    url_hash,
)
from scraper.enrichment.language_detector import detect_language
from scraper.enrichment.ticker_extractor import has_financial_content
from scraper.extractors import TrafilaturaExtractor
from scraper.fetchers import get_fetcher
from scraper.models.article import Article, RawArticle
from scraper.models.source import SourceConfig
from scraper.quality import QualityPipeline

logger = logging.getLogger(__name__)


@dataclass
class RunStats:
    found: int = 0
    stored: int = 0
    duped: int = 0
    near_duped: int = 0
    failed: int = 0
    metadata_only: int = 0
    errors: list[str] = field(default_factory=list)


class Orchestrator:
    """Wires fetch → extract → validate → enrich → store."""

    def __init__(self):
        self._extractor = TrafilaturaExtractor()

    def run(
        self,
        source_config: SourceConfig,
        run_type: str = "realtime",
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        ticker: Optional[str] = None,
        url_cursor: int = 0,
    ) -> RunStats:
        stats = RunStats()

        with get_session() as session:
            src_repo = SourceRepository(session)
            run_repo = ScrapeRunRepository(session)

            source_orm = src_repo.get_or_create(source_config.name, source_config.type)
            run = run_repo.create(source_config.name, run_type, source_orm.id)

            try:
                fetcher = get_fetcher(source_config)
            except Exception as exc:
                run_repo.finish(run, "failed", 0, 0, 0, 0, str(exc))
                logger.error("Failed to create fetcher for %s: %s", source_config.name, exc)
                return stats

            try:
                raw_articles = fetcher.fetch(
                    from_dt=from_dt,
                    to_dt=to_dt,
                    ticker=ticker,
                    url_cursor=url_cursor,
                )
            except Exception as exc:
                run_repo.finish(run, "failed", 0, 0, 0, 1, str(exc))
                logger.error("Fetch failed for %s: %s", source_config.name, exc)
                stats.failed += 1
                return stats

            stats.found = len(raw_articles)
            art_repo = ArticleRepository(session)

            quality = QualityPipeline(
                db_url_hash_fn=art_repo.url_hash_exists,
                db_content_hash_fn=art_repo.content_hash_exists,
                db_simhashes_fn=art_repo.recent_simhashes,
            )

            for raw in raw_articles:
                try:
                    # Savepoint: one article failure cannot roll back the entire batch
                    with session.begin_nested():
                        result = self._process_one(
                            raw, source_orm.id, source_config, art_repo, quality
                        )
                    stats.stored += result["stored"]
                    stats.duped += result["duped"]
                    stats.near_duped += result["near_duped"]
                    stats.metadata_only += result["metadata_only"]
                    stats.failed += result["failed"]
                except Exception as exc:
                    logger.warning("Error processing %s: %s", raw.url, exc)
                    stats.errors.append(str(exc))
                    stats.failed += 1

            status = "completed" if stats.failed < max(stats.found, 1) else "failed"
            run_repo.finish(
                run, status,
                stats.found, stats.stored, stats.duped, stats.failed,
            )

        logger.info(
            "[%s] found=%d stored=%d duped=%d near_duped=%d meta_only=%d failed=%d",
            source_config.name, stats.found, stats.stored, stats.duped,
            stats.near_duped, stats.metadata_only, stats.failed,
        )
        return stats

    def _process_one(
        self,
        raw: RawArticle,
        source_id: int,
        config: SourceConfig,
        art_repo: ArticleRepository,
        quality: QualityPipeline,
    ) -> dict:
        result = {"stored": 0, "duped": 0, "near_duped": 0, "metadata_only": 0, "failed": 0}

        uh = url_hash(raw.url)
        # Quick URL dedup before any I/O
        if art_repo.url_hash_exists(uh):
            result["duped"] = 1
            return result
        # External ID dedup (catches same article re-fetched with different URL tracking params)
        if raw.external_id and art_repo.external_id_exists(config.name, raw.external_id):
            result["duped"] = 1
            return result

        # Determine if we should fetch full text
        is_metadata_only = (
            config.metadata_only
            or is_paywalled_domain(raw.url)
            or is_scraper_blocked(raw.url)
        )

        body = ""
        extraction = None

        if not is_metadata_only:
            extraction = self._extractor.extract(raw.url)
            body = extraction.body if extraction else ""
            # If extraction failed completely, fall back to RSS summary (metadata-only)
            if not body and raw.summary:
                body = raw.summary
                is_metadata_only = True
                logger.debug("Falling back to metadata-only for %s", raw.url)
        else:
            # Use RSS summary as body for metadata-only articles
            body = raw.summary or raw.title

        # Apply PR Newswire / financial_filter check
        if config.financial_filter and not has_financial_content(body, raw.title):
            result["failed"] = 1
            logger.debug("Financial filter rejected: %s", raw.title[:60])
            return result

        ch = content_hash(body) if body else None
        sh = compute_simhash(body) if body and not is_metadata_only else None

        result_q = quality.run(
            raw, body, uh, ch or "",
            simhash=sh,
            is_metadata_only=is_metadata_only,
        )

        if not result_q.passed:
            if result_q.is_duplicate:
                result["duped"] = 1
            else:
                result["failed"] = 1
            return result

        if result_q.is_near_duplicate:
            result["near_duped"] = 1

        # Enrich
        tickers = extract_tickers(raw.title + " " + body)
        lang, conf = ("en", 1.0)
        if body and not is_metadata_only:
            lang, conf = detect_language(body)

        title = raw.title
        author = raw.author
        if extraction:
            title = extraction.title or raw.title
            author = extraction.author or raw.author

        article = Article(
            url=raw.url,
            url_hash=uh,
            content_hash=ch,
            title=title,
            body=body,
            summary=raw.summary or (body[:500] if body else ""),
            word_count=len(body.split()),
            source_name=config.name,
            source_id=source_id,
            author=author,
            language=lang,
            published_at=raw.published_at,
            scraped_at=datetime.now(timezone.utc),
            tickers=tickers,
            quality_score=result_q.quality_score,
            quality_flags=result_q.flags,
            is_paywall=result_q.is_paywall,
            is_duplicate=result_q.is_duplicate,
            is_near_duplicate=result_q.is_near_duplicate,
            is_metadata_only=is_metadata_only,
            simhash=sh,
            external_id=raw.external_id,
            raw_json=raw.raw_json,
        )

        _, was_inserted = art_repo.upsert(article)
        if was_inserted:
            result["stored"] = 1
            if is_metadata_only:
                result["metadata_only"] = 1
        else:
            result["duped"] = 1

        return result


class BackfillOrchestrator(Orchestrator):
    """Handles resumable backfills with cursor tracking."""

    def run_backfill(
        self,
        source_config: SourceConfig,
        from_dt: datetime,
        to_dt: datetime,
        workers: int = 1,
    ) -> RunStats:
        url_cursor = 0
        with get_session() as session:
            run_repo = ScrapeRunRepository(session)
            last_run = run_repo.get_last_backfill(source_config.name)
            if last_run and last_run.status != "completed":
                meta = last_run.get_metadata()
                url_cursor = meta.get("url_cursor", 0)
                logger.info(
                    "Resuming backfill for %s from cursor %d",
                    source_config.name, url_cursor,
                )

        stats = RunStats()

        with get_session() as session:
            src_repo = SourceRepository(session)
            run_repo = ScrapeRunRepository(session)
            art_repo = ArticleRepository(session)

            source_orm = src_repo.get_or_create(source_config.name, source_config.type)
            run = run_repo.create(source_config.name, "backfill", source_orm.id)
            run.set_metadata({"date_cursor": from_dt.isoformat(), "url_cursor": url_cursor})
            session.flush()

            quality = QualityPipeline(
                db_url_hash_fn=art_repo.url_hash_exists,
                db_content_hash_fn=art_repo.content_hash_exists,
                db_simhashes_fn=art_repo.recent_simhashes,
            )

            fetcher = get_fetcher(source_config)
            iter_fn = getattr(fetcher, "iter_articles_resumable", None)

            if iter_fn is None:
                raw_articles = fetcher.fetch(from_dt=from_dt, to_dt=to_dt, url_cursor=url_cursor)
                stats.found = len(raw_articles)
                for raw in raw_articles:
                    try:
                        r = self._process_one(raw, source_orm.id, source_config, art_repo, quality)
                        stats.stored += r["stored"]
                        stats.duped += r["duped"]
                        stats.near_duped += r["near_duped"]
                        stats.failed += r["failed"]
                    except Exception as exc:
                        stats.failed += 1
                        logger.warning("Backfill error %s: %s", raw.url, exc)
            else:
                for raw, cursor in iter_fn(from_dt, to_dt, url_cursor):
                    stats.found += 1
                    try:
                        r = self._process_one(raw, source_orm.id, source_config, art_repo, quality)
                        stats.stored += r["stored"]
                        stats.duped += r["duped"]
                        stats.near_duped += r["near_duped"]
                        stats.failed += r["failed"]
                    except Exception as exc:
                        stats.failed += 1
                        logger.warning("Backfill error %s: %s", raw.url, exc)

                    if stats.found % 1_000 == 0:
                        run.set_metadata({
                            "date_cursor": from_dt.isoformat(),
                            "url_cursor": cursor,
                        })
                        session.flush()
                        logger.info(
                            "Backfill checkpoint [%s] cursor=%d stored=%d",
                            source_config.name, cursor, stats.stored,
                        )

            run_repo.finish(
                run, "completed",
                stats.found, stats.stored, stats.duped, stats.failed,
            )

        return stats
