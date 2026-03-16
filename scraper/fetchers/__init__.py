from .base import BaseFetcher, FetchError
from .rss import RSSFetcher
from .newsapi import NewsAPIFetcher
from .finnhub import FinnhubFetcher
from .gdelt import GDELTFetcher
from .wayback import WaybackFetcher
from scraper.models.source import SourceConfig


def get_fetcher(config: SourceConfig) -> BaseFetcher:
    """Factory — returns the right fetcher for a source type."""
    mapping = {
        "rss": RSSFetcher,
        "api": _api_fetcher,
        "gdelt": GDELTFetcher,
        "wayback": WaybackFetcher,
    }
    factory = mapping.get(config.type)
    if factory is None:
        raise ValueError(f"Unknown source type: {config.type}")
    if callable(factory) and factory is _api_fetcher:
        return _api_fetcher(config)
    return factory(config)  # type: ignore[operator]


def _api_fetcher(config: SourceConfig) -> BaseFetcher:
    if config.name.startswith("newsapi"):
        return NewsAPIFetcher(config)
    if config.name.startswith("finnhub"):
        return FinnhubFetcher(config)
    raise ValueError(f"No API fetcher for source: {config.name}")


__all__ = [
    "BaseFetcher",
    "FetchError",
    "RSSFetcher",
    "NewsAPIFetcher",
    "FinnhubFetcher",
    "GDELTFetcher",
    "WaybackFetcher",
    "get_fetcher",
]
