"""Core Scraper class — the single public entry point for the finews library."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from finews._sources import BUILTIN_SOURCE_NAMES, BUILTIN_SOURCES
from scraper.db.base import Base
from scraper.db.repository import ArticleRepository
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
from scraper.fetchers import BaseFetcher, get_fetcher
from scraper.models.article import Article, RawArticle
from scraper.models.source import SourceConfig
from scraper.quality import QualityPipeline

logger = logging.getLogger(__name__)


class _InMemoryStore:
    """Lightweight in-process dedup store used when no DB is requested.

    Mirrors the three callable signatures that :class:`QualityPipeline`
    expects, so we can plug it in without touching internals.
    """

    def __init__(self) -> None:
        self._url_hashes: set[str] = set()
        self._content_hashes: set[str] = set()
        self._ext_ids: set[tuple[str, str]] = set()
        self._simhashes: list[str] = []

    # ── QualityPipeline-compatible callables ──────────────────────────────────

    def url_hash_exists(self, h: str) -> bool:
        return h in self._url_hashes

    def content_hash_exists(self, h: str) -> bool:
        return h in self._content_hashes

    def recent_simhashes(self) -> list[str]:
        return list(self._simhashes)

    # ── Internal bookkeeping ──────────────────────────────────────────────────

    def external_id_exists(self, source: str, ext_id: str) -> bool:
        return (source, ext_id) in self._ext_ids

    def add(self, article: Article) -> None:
        self._url_hashes.add(article.url_hash)
        if article.content_hash:
            self._content_hashes.add(article.content_hash)
        if article.external_id:
            self._ext_ids.add((article.source_name, article.external_id))
        if article.simhash:
            self._simhashes.append(article.simhash)


class Scraper:
    """High-level API for fetching enriched financial news articles.

    Parameters
    ----------
    sources:
        Which sources to fetch from.  Each item is either a built-in source
        name (string) or a custom :class:`~scraper.fetchers.BaseFetcher`
        instance.

        Built-in names (RSS, free):
            ``"yahoofinance"``, ``"cnbc"``, ``"motleyfool"``, ``"benzinga"``,
            ``"businessinsider"``, ``"fortune"``, ``"prnewswire"``

        Metadata-only (paywalled — title + summary only):
            ``"bloomberg"``, ``"wsj"``, ``"ft"``, ``"seekingalpha"``

        API sources (key required):
            ``"newsapi"``, ``"finnhub"``

        Defaults to all built-in RSS sources when omitted.

    newsapi_key:
        NewsAPI.org API key.  Required when ``"newsapi"`` is in *sources*.
    finnhub_api_key:
        Finnhub API key.  Required when ``"finnhub"`` is in *sources*.

    Examples
    --------
    Basic usage::

        from finews import Scraper

        scraper = Scraper(sources=["yahoofinance", "cnbc"])
        articles = scraper.fetch(days_back=1)
        for a in articles:
            print(a.title, a.tickers, a.quality_score)

    With API sources and ticker filter::

        scraper = Scraper(
            sources=["newsapi", "finnhub"],
            newsapi_key="YOUR_KEY",
            finnhub_api_key="YOUR_KEY",
        )
        articles = scraper.fetch(tickers=["AAPL", "TSLA"], days_back=7)

    Persist to SQLite::

        articles = scraper.fetch(save_to="sqlite:///./financial_news.db")

    Custom fetcher::

        from scraper.fetchers import BaseFetcher

        class MyFetcher(BaseFetcher):
            def fetch(self, **kwargs):
                ...

        scraper = Scraper(sources=[MyFetcher(config), "cnbc"])
    """

    def __init__(
        self,
        sources: list | None = None,
        *,
        newsapi_key: str = "",
        finnhub_api_key: str = "",
    ) -> None:
        # Inject API keys into the global settings singleton.
        # BaseFetcher subclasses read from settings at __init__ time.
        if newsapi_key:
            settings.newsapi_key = newsapi_key
        if finnhub_api_key:
            settings.finnhub_api_key = finnhub_api_key

        # Default: all built-in RSS sources (no API key required)
        if sources is None:
            sources = [k for k, v in BUILTIN_SOURCES.items() if v.type == "rss"]

        self._source_configs: list[SourceConfig] = []
        self._custom_fetchers: list[BaseFetcher] = []

        for s in sources:
            if isinstance(s, BaseFetcher):
                self._custom_fetchers.append(s)
            elif isinstance(s, str):
                if s not in BUILTIN_SOURCE_NAMES:
                    raise ValueError(
                        f"Unknown source {s!r}. "
                        f"Available built-in sources: {sorted(BUILTIN_SOURCE_NAMES)}"
                    )
                self._source_configs.append(BUILTIN_SOURCES[s])
            else:
                raise TypeError(f"sources items must be str or BaseFetcher, got {type(s).__name__}")

    # ── Public API ────────────────────────────────────────────────────────────

    def fetch(
        self,
        *,
        tickers: list[str] | None = None,
        days_back: int = 1,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        min_quality: float = 0.0,
        limit: int | None = None,
        save_to: str | None = None,
    ) -> list[Article]:
        """Fetch, enrich, and return articles from all configured sources.

        Parameters
        ----------
        tickers:
            Return only articles whose extracted tickers overlap with this
            list (e.g. ``["AAPL", "MSFT"]``).  Case-insensitive.
        days_back:
            Number of calendar days back to fetch.  Ignored when *from_dt*
            is supplied explicitly.
        from_dt / to_dt:
            Explicit UTC date range.  *to_dt* defaults to now.
        min_quality:
            Discard articles below this quality score (0.0–1.0).
        limit:
            Cap the returned list to this many articles (most-recent first).
        save_to:
            SQLAlchemy DB URL (e.g. ``"sqlite:///./news.db"`` or
            ``"postgresql://user:pw@host/db"``).  When provided, all
            returned articles are also persisted to that database.
            Tables are created automatically if they do not exist.

        Returns
        -------
        list[Article]
            Fully enriched :class:`~scraper.models.article.Article` objects,
            sorted newest-first.
        """
        now = datetime.now(timezone.utc)
        if from_dt is None:
            from_dt = now - timedelta(days=days_back)
        if to_dt is None:
            to_dt = now

        store = _InMemoryStore()
        extractor = TrafilaturaExtractor()
        articles: list[Article] = []

        for config in self._source_configs:
            batch = self._run_source(config, from_dt, to_dt, tickers, store, extractor)
            articles.extend(batch)

        for fetcher in self._custom_fetchers:
            batch = self._run_source(
                fetcher.config,
                from_dt,
                to_dt,
                tickers,
                store,
                extractor,
                fetcher=fetcher,
            )
            articles.extend(batch)

        # ── Post-processing ───────────────────────────────────────────────────
        if tickers:
            upper = {t.upper() for t in tickers}
            articles = [a for a in articles if upper.intersection(a.tickers)]

        if min_quality > 0.0:
            articles = [a for a in articles if a.quality_score >= min_quality]

        articles.sort(key=lambda a: a.published_at, reverse=True)

        if limit is not None:
            articles = articles[:limit]

        if save_to:
            self._persist(articles, save_to)

        return articles

    # ── Internal pipeline ─────────────────────────────────────────────────────

    def _run_source(
        self,
        config: SourceConfig,
        from_dt: datetime,
        to_dt: datetime,
        tickers: list[str] | None,
        store: _InMemoryStore,
        extractor: TrafilaturaExtractor,
        *,
        fetcher: BaseFetcher | None = None,
    ) -> list[Article]:
        # For single-ticker API sources (e.g. Finnhub company-news endpoint),
        # pass the ticker directly.  Multi-ticker filtering happens after.
        ticker_hint = tickers[0] if tickers and len(tickers) == 1 else None

        try:
            if fetcher is None:
                fetcher = get_fetcher(config)
            raw_articles = fetcher.fetch(from_dt=from_dt, to_dt=to_dt, ticker=ticker_hint)
        except Exception as exc:
            logger.warning("Fetch failed for %s: %s", config.name, exc)
            return []

        quality = QualityPipeline(
            db_url_hash_fn=store.url_hash_exists,
            db_content_hash_fn=store.content_hash_exists,
            db_simhashes_fn=store.recent_simhashes,
        )

        results: list[Article] = []
        for raw in raw_articles:
            article = self._process_one(raw, config, store, extractor, quality)
            if article is not None:
                results.append(article)

        logger.info("[%s] fetched=%d kept=%d", config.name, len(raw_articles), len(results))
        return results

    def _process_one(
        self,
        raw: RawArticle,
        config: SourceConfig,
        store: _InMemoryStore,
        extractor: TrafilaturaExtractor,
        quality: QualityPipeline,
    ) -> Article | None:
        uh = url_hash(raw.url)

        # URL-level dedup
        if store.url_hash_exists(uh):
            return None
        if raw.external_id and store.external_id_exists(config.name, raw.external_id):
            return None

        is_metadata_only = (
            config.metadata_only or is_paywalled_domain(raw.url) or is_scraper_blocked(raw.url)
        )

        body = ""
        extraction = None

        if not is_metadata_only:
            extraction = extractor.extract(raw.url)
            body = extraction.body if extraction else ""
            if not body and raw.summary:
                body = raw.summary
                is_metadata_only = True
        else:
            body = raw.summary or raw.title

        if config.financial_filter and not has_financial_content(body, raw.title):
            return None

        ch = content_hash(body) if body else None
        sh = compute_simhash(body) if body and not is_metadata_only else None

        result_q = quality.run(
            raw,
            body,
            uh,
            ch or "",
            simhash=sh,
            is_metadata_only=is_metadata_only,
        )
        if not result_q.passed:
            return None

        tickers = extract_tickers(raw.title + " " + body)

        lang = "en"
        if body and not is_metadata_only:
            lang, _ = detect_language(body)

        title = extraction.title if (extraction and extraction.title) else raw.title
        author = extraction.author if (extraction and extraction.author) else raw.author

        article = Article(
            url=raw.url,
            url_hash=uh,
            content_hash=ch,
            title=title,
            body=body,
            summary=raw.summary or (body[:500] if body else ""),
            word_count=len(body.split()),
            source_name=config.name,
            author=author,
            language=lang,
            published_at=raw.published_at,
            scraped_at=datetime.now(timezone.utc),
            tickers=tickers,
            quality_score=result_q.quality_score,
            quality_flags=result_q.flags,
            is_paywall=result_q.is_paywall,
            is_duplicate=False,
            is_near_duplicate=result_q.is_near_duplicate,
            is_metadata_only=is_metadata_only,
            simhash=sh,
            external_id=raw.external_id,
            raw_json=raw.raw_json,
        )

        store.add(article)
        return article

    # ── Optional persistence ──────────────────────────────────────────────────

    def _persist(self, articles: list[Article], db_url: str) -> None:
        """Upsert *articles* into *db_url*, creating tables if needed."""
        kwargs: dict = {}
        if db_url.startswith("sqlite"):
            kwargs["connect_args"] = {"check_same_thread": False}

        eng = create_engine(db_url, **kwargs)
        Base.metadata.create_all(eng)
        Session = sessionmaker(bind=eng, expire_on_commit=False)

        with Session() as session:
            repo = ArticleRepository(session)
            stored = 0
            for article in articles:
                try:
                    _, inserted = repo.upsert(article)
                    stored += int(inserted)
                except Exception as exc:
                    logger.warning("Persist failed for %s: %s", article.url, exc)
            session.commit()

        logger.info("Persisted %d/%d articles to %s", stored, len(articles), db_url)
