from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from config.settings import settings
from scraper.models.article import RawArticle
from scraper.models.source import SourceConfig
from .base import BaseFetcher, FetchError

logger = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"


class FinnhubFetcher(BaseFetcher):
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        if not settings.finnhub_api_key:
            raise FetchError("FINNHUB_API_KEY not set in environment")
        self._api_key = settings.finnhub_api_key

    def fetch(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        ticker: Optional[str] = None,
        **kwargs,
    ) -> list[RawArticle]:
        if ticker:
            return self._fetch_company_news(ticker, from_dt, to_dt)
        return self._fetch_general_news()

    def _fetch_general_news(self) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for category in self.config.categories:
            try:
                articles.extend(self._fetch_category(category))
            except Exception as exc:
                logger.warning("Finnhub category '%s' failed: %s", category, exc)
        return articles

    def _fetch_category(self, category: str) -> list[RawArticle]:
        resp = self._get(
            f"{FINNHUB_BASE}/news",
            params={"category": category, "token": self._api_key},
        )
        return self._parse_items(resp.json())

    def _fetch_company_news(
        self,
        ticker: str,
        from_dt: Optional[datetime],
        to_dt: Optional[datetime],
    ) -> list[RawArticle]:
        now = datetime.now(timezone.utc)
        params = {
            "symbol": ticker,
            "from": (from_dt or now).strftime("%Y-%m-%d"),
            "to": (to_dt or now).strftime("%Y-%m-%d"),
            "token": self._api_key,
        }
        resp = self._get(f"{FINNHUB_BASE}/company-news", params=params)
        items = self._parse_items(resp.json())
        # Tag all items with the requested ticker
        for item in items:
            item.external_id = f"{ticker}:{item.external_id}"
        return items

    def _parse_items(self, data: list) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for item in data:
            url = item.get("url")
            headline = item.get("headline") or item.get("title")
            if not url or not headline:
                continue
            try:
                pub_ts = item.get("datetime", 0)
                pub_dt = datetime.fromtimestamp(pub_ts, tz=timezone.utc)
                article = RawArticle(
                    url=url,
                    title=headline.strip(),
                    summary=item.get("summary"),
                    published_at=pub_dt,
                    source_name=self.config.name,
                    author=None,
                    external_id=str(item.get("id", url)),
                    raw_json=json.dumps(item),
                )
                articles.append(article)
            except Exception as exc:
                logger.debug("Skipping Finnhub item %s: %s", url, exc)

        logger.info("Finnhub: parsed %d articles", len(articles))
        return articles
