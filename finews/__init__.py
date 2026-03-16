"""finews — Financial news data library for Python.

Quickstart::

    from finews import Scraper

    scraper = Scraper(sources=["yahoofinance", "cnbc"])
    articles = scraper.fetch(tickers=["AAPL"], days_back=7)

    for a in articles:
        print(a.title, a.tickers, a.quality_score)

Building a custom source::

    from finews import Scraper, BaseFetcher, SourceConfig
    from scraper.models.article import RawArticle

    class MySource(BaseFetcher):
        def __init__(self, api_key: str):
            super().__init__(SourceConfig(name="my_source", type="api"))
            self._api_key = api_key

        def fetch(self, from_dt=None, to_dt=None, ticker=None, **kwargs):
            ...  # return list[RawArticle]

    scraper = Scraper(sources=[MySource(api_key="secret"), "cnbc"])
    articles = scraper.fetch(tickers=["AAPL"])

See :class:`Scraper` and :class:`BaseFetcher` for the full API reference.
"""

from finews._scraper import Scraper
from finews._sources import BUILTIN_SOURCE_NAMES, BUILTIN_SOURCES
from scraper.fetchers.base import BaseFetcher, FetchError
from scraper.models.article import Article
from scraper.models.source import SourceConfig

__all__ = [
    # Core API
    "Scraper",
    "Article",
    # Custom source building blocks
    "BaseFetcher",
    "FetchError",
    "SourceConfig",
    # Source registry
    "BUILTIN_SOURCES",
    "BUILTIN_SOURCE_NAMES",
]

__version__ = "0.1.0"
