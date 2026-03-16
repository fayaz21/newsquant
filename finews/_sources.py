"""Built-in source registry.

Maps friendly short names (e.g. ``"yahoofinance"``) to pre-configured
:class:`~scraper.models.source.SourceConfig` objects.  This is the single
source of truth for what ``Scraper(sources=[...])`` accepts.
"""
from __future__ import annotations

from scraper.models.source import SourceConfig

BUILTIN_SOURCES: dict[str, SourceConfig] = {
    # ── Tier 1: full-text RSS ─────────────────────────────────────────────────
    "yahoofinance": SourceConfig(
        name="yahoofinance_rss",
        type="rss",
        tier=1,
        rate_limit_rps=1.0,
        feeds=[
            "https://finance.yahoo.com/news/rssindex",
            "https://finance.yahoo.com/rss/topstories",
        ],
    ),
    "cnbc": SourceConfig(
        name="cnbc_rss",
        type="rss",
        tier=1,
        rate_limit_rps=1.0,
        feeds=[
            "https://www.cnbc.com/id/10001147/device/rss/rss.html",
            "https://www.cnbc.com/id/10000664/device/rss/rss.html",
            "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        ],
    ),
    "motleyfool": SourceConfig(
        name="motleyfool_rss",
        type="rss",
        tier=1,
        rate_limit_rps=0.5,
        feeds=["https://www.fool.com/feeds/index.aspx"],
    ),
    "benzinga": SourceConfig(
        name="benzinga_rss",
        type="rss",
        tier=1,
        rate_limit_rps=0.5,
        feeds=[
            "https://www.benzinga.com/feed",
            "https://www.benzinga.com/feeds/news",
        ],
    ),
    "businessinsider": SourceConfig(
        name="businessinsider_rss",
        type="rss",
        tier=1,
        rate_limit_rps=0.5,
        feeds=["https://feeds.businessinsider.com/custom/all"],
    ),
    "fortune": SourceConfig(
        name="fortune_rss",
        type="rss",
        tier=1,
        rate_limit_rps=0.5,
        feeds=["https://fortune.com/feed/"],
    ),
    "prnewswire": SourceConfig(
        name="prnewswire_rss",
        type="rss",
        tier=1,
        rate_limit_rps=0.5,
        financial_filter=True,
        feeds=[
            "https://www.prnewswire.com/rss/news-releases-list.rss?category=EPSF",
            "https://www.prnewswire.com/rss/news-releases-list.rss?category=MLAW",
        ],
    ),
    # ── Tier 2: metadata-only (paywalled) ────────────────────────────────────
    "bloomberg": SourceConfig(
        name="bloomberg_rss",
        type="rss",
        tier=2,
        metadata_only=True,
        full_text_required=False,
        rate_limit_rps=1.0,
        feeds=[
            "https://feeds.bloomberg.com/markets/news.rss",
            "https://feeds.bloomberg.com/technology/news.rss",
        ],
    ),
    "wsj": SourceConfig(
        name="wsj_rss",
        type="rss",
        tier=2,
        metadata_only=True,
        full_text_required=False,
        rate_limit_rps=1.0,
        feeds=[
            "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
            "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
        ],
    ),
    "ft": SourceConfig(
        name="ft_rss",
        type="rss",
        tier=2,
        metadata_only=True,
        full_text_required=False,
        rate_limit_rps=0.5,
        feeds=[
            "https://www.ft.com/rss/home/uk",
            "https://www.ft.com/myft/following/c42b7996-2da5-400a-b64a-d9bff7982790/rss",
        ],
    ),
    "seekingalpha": SourceConfig(
        name="seekingalpha_rss",
        type="rss",
        tier=2,
        metadata_only=True,
        full_text_required=False,
        rate_limit_rps=0.5,
        feeds=["https://seekingalpha.com/market_currents.xml"],
    ),
    # ── API sources ───────────────────────────────────────────────────────────
    "newsapi": SourceConfig(
        name="newsapi",
        type="api",
        tier=1,
        rate_limit_rps=0.5,
        full_text_required=False,
        queries=["stock market", "earnings", "Fed rate", "IPO", "merger acquisition"],
        language="en",
        page_size=100,
    ),
    "finnhub": SourceConfig(
        name="finnhub",
        type="api",
        tier=1,
        rate_limit_rps=1.0,
        full_text_required=False,
        categories=["general", "forex", "merger"],
    ),
}

#: Convenience set of all valid source names for validation messages.
BUILTIN_SOURCE_NAMES: frozenset[str] = frozenset(BUILTIN_SOURCES.keys())
