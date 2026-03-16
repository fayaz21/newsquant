"""Source configuration model."""
from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class SourceConfig(BaseModel):
    """Configuration for a single news source.

    Pass an instance of this to a :class:`~scraper.fetchers.BaseFetcher`
    subclass to describe how the pipeline should treat articles from that
    source.

    Required fields
    ---------------
    name:
        Unique identifier used in logs and the database (e.g. ``"my_api"``).
    type:
        Source type: ``"rss"``, ``"api"``, ``"gdelt"``, or ``"wayback"``.

    Common fields for custom sources
    ---------------------------------
    rate_limit_rps:
        Maximum requests per second (default ``1.0``).  The :meth:`BaseFetcher._rate_limit`
        helper enforces this automatically.
    metadata_only:
        Set ``True`` for paywalled sources where full-text extraction will
        fail.  The pipeline stores the RSS/API summary as the article body and
        caps the quality score at ``0.75``.
    financial_filter:
        Set ``True`` to reject articles with no detectable financial content
        (useful for broad wire services like PR Newswire).

    Source-specific fields
    ----------------------
    feeds:
        List of RSS feed URLs.  Used by ``RSSFetcher``.
    queries:
        Search query strings.  Used by ``NewsAPIFetcher``.
    categories:
        News categories.  Used by ``FinnhubFetcher``.
    financial_themes:
        GDELT theme prefixes (e.g. ``["ECON_", "MARKET_"]``).  Used by ``GDELTFetcher``.
    domains:
        Domain names for Wayback Machine crawl (e.g. ``["reuters.com"]``).  Used by ``WaybackFetcher``.

    Example
    -------
    Minimal config for a custom API fetcher::

        from scraper.models.source import SourceConfig

        config = SourceConfig(
            name="my_api",
            type="api",
            rate_limit_rps=2.0,
        )
    """

    name: str
    type: str = Field(description="rss | api | gdelt | wayback")
    tier: int = Field(default=1, description="1 = full-text, 2 = metadata-only")
    enabled: bool = True
    full_text_required: bool = True
    metadata_only: bool = Field(
        default=False,
        description="Skip full-text extraction; use RSS summary as body.",
    )
    financial_filter: bool = Field(
        default=False,
        description="Reject articles with no detectable financial content.",
    )
    rate_limit_rps: float = Field(
        default=1.0,
        description="Max requests per second. 0 disables rate limiting.",
    )
    schedule_cron: Optional[str] = None
    backfill_only: bool = False

    # ── Source-specific fields ────────────────────────────────────────────────
    # RSS
    feeds: list[str] = Field(default=[], description="RSS feed URLs (RSSFetcher).")

    # NewsAPI
    queries: list[str] = Field(default=[], description="Search queries (NewsAPIFetcher).")
    language: str = "en"
    page_size: int = 100

    # Finnhub
    categories: list[str] = Field(default=[], description="News categories (FinnhubFetcher).")

    # GDELT
    financial_themes: list[str] = Field(
        default=[],
        description="GDELT theme prefixes, e.g. ['ECON_', 'MARKET_'] (GDELTFetcher).",
    )
    batch_days: int = 7

    # Wayback
    domains: list[str] = Field(
        default=[],
        description="Domains to crawl via Wayback CDX API (WaybackFetcher).",
    )
