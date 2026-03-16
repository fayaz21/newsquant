"""Base class and shared HTTP machinery for all fetchers."""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime

import httpx
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings
from scraper.models.article import RawArticle
from scraper.models.source import SourceConfig

logger = logging.getLogger(__name__)


class FetchError(Exception):
    """Raised when a fetcher cannot retrieve articles (bad key, network error, etc.)."""


class BaseFetcher(ABC):
    """Abstract base class for all news fetchers.

    Subclass this to add a custom source to :class:`~finews.Scraper`.

    Contract
    --------
    1. Override :meth:`fetch` — return a flat list of
       :class:`~scraper.models.article.RawArticle` objects.
    2. Store your configuration (URLs, keys, etc.) on ``self.config`` using
       a :class:`~scraper.models.source.SourceConfig`.
    3. Use :meth:`_get` / :meth:`_post` for HTTP calls — they handle rate
       limiting, retries, and timeouts automatically.
    4. Raise :exc:`FetchError` for unrecoverable errors (bad API key, etc.).
       Other exceptions are caught and logged by the pipeline.

    Minimal example::

        from finews import BaseFetcher, SourceConfig
        from scraper.models.article import RawArticle

        class MyAPIFetcher(BaseFetcher):
            def __init__(self, api_key: str):
                config = SourceConfig(
                    name="my_api",
                    type="api",
                    rate_limit_rps=2.0,
                )
                super().__init__(config)
                self._api_key = api_key

            def fetch(
                self,
                from_dt=None,
                to_dt=None,
                ticker=None,
                **kwargs,
            ) -> list[RawArticle]:
                resp = self._get(
                    "https://api.example.com/news",
                    params={"key": self._api_key, "symbol": ticker},
                )
                return [
                    RawArticle(
                        url=item["url"],
                        title=item["title"],
                        published_at=item["published_at"],
                        source_name=self.config.name,
                    )
                    for item in resp.json()["articles"]
                ]

    Then plug it in::

        from finews import Scraper

        scraper = Scraper(sources=[MyAPIFetcher(api_key="secret")])
        articles = scraper.fetch(tickers=["AAPL"])
    """

    def __init__(self, config: SourceConfig) -> None:
        self.config = config
        self._last_request_at: float = 0.0

    @abstractmethod
    def fetch(
        self,
        from_dt: datetime | None = None,
        to_dt: datetime | None = None,
        ticker: str | None = None,
        **kwargs,
    ) -> list[RawArticle]:
        """Fetch and return raw articles from this source.

        The pipeline always calls ``fetch`` with keyword arguments.  You are
        not required to use every parameter — ignore the ones that don't apply
        to your source.

        Parameters
        ----------
        from_dt:
            Fetch articles published on or after this UTC datetime.
        to_dt:
            Fetch articles published on or before this UTC datetime.
        ticker:
            Single ticker symbol hint (e.g. ``"AAPL"``).  Passed through when
            the caller requests a single ticker and the source supports
            company-level queries (e.g. Finnhub company-news endpoint).
            Ignore this if your source does not support per-ticker queries.
        **kwargs:
            Source-specific extras (e.g. ``url_cursor`` for backfill sources).

        Returns
        -------
        list[RawArticle]
            Raw, unvalidated articles.  The pipeline handles extraction,
            enrichment, deduplication, and quality scoring.
        """
        ...

    # ── Protected HTTP helpers ────────────────────────────────────────────────
    # Use these in your fetch() implementation.  They handle rate limiting,
    # exponential-backoff retries, and timeouts automatically.

    def _rate_limit(self) -> None:
        """Block until the configured ``rate_limit_rps`` interval has passed.

        Called automatically by :meth:`_get` and :meth:`_post`.  You can also
        call it manually before non-HTTP I/O (e.g. a third-party SDK call).
        """
        if self.config.rate_limit_rps <= 0:
            return
        min_interval = 1.0 / self.config.rate_limit_rps
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_at = time.monotonic()

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(settings.max_retries),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _get(self, url: str, **kwargs) -> httpx.Response:
        """HTTP GET with automatic rate limiting and retry.

        Parameters
        ----------
        url:
            The URL to request.
        **kwargs:
            Forwarded to :func:`httpx.Client.get` (e.g. ``params``, ``headers``).

        Returns
        -------
        httpx.Response
            The successful response (2xx).  Non-2xx raises ``httpx.HTTPStatusError``,
            which triggers a retry up to ``settings.max_retries`` times.
        """
        self._rate_limit()
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.get(url, **kwargs)
            resp.raise_for_status()
            return resp

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(settings.max_retries),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _post(self, url: str, **kwargs) -> httpx.Response:
        """HTTP POST with automatic rate limiting and retry.

        Parameters
        ----------
        url:
            The URL to request.
        **kwargs:
            Forwarded to :func:`httpx.Client.post` (e.g. ``json``, ``data``, ``headers``).

        Returns
        -------
        httpx.Response
            The successful response (2xx).
        """
        self._rate_limit()
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.post(url, **kwargs)
            resp.raise_for_status()
            return resp
