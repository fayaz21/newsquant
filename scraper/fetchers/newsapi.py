from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional

from config.settings import settings
from scraper.models.article import RawArticle
from scraper.models.source import SourceConfig
from .base import BaseFetcher, FetchError

logger = logging.getLogger(__name__)

NEWSAPI_BASE = "https://newsapi.org/v2/everything"


class NewsAPIFetcher(BaseFetcher):
    def __init__(self, config: SourceConfig):
        super().__init__(config)
        if not settings.newsapi_key:
            raise FetchError("NEWSAPI_KEY not set in environment")
        self._api_key = settings.newsapi_key

    def fetch(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        page: int = 1,
        **kwargs,
    ) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for query in self.config.queries:
            try:
                articles.extend(self._fetch_query(query, from_dt, to_dt, page))
            except Exception as exc:
                logger.warning("NewsAPI query '%s' failed: %s", query, exc)
        return articles

    def _fetch_query(
        self,
        query: str,
        from_dt: Optional[datetime],
        to_dt: Optional[datetime],
        page: int,
    ) -> list[RawArticle]:
        params: dict = {
            "q": query,
            "language": self.config.language,
            "pageSize": self.config.page_size,
            "page": page,
            "sortBy": "publishedAt",
            "apiKey": self._api_key,
        }
        if from_dt:
            params["from"] = from_dt.strftime("%Y-%m-%dT%H:%M:%S")
        if to_dt:
            params["to"] = to_dt.strftime("%Y-%m-%dT%H:%M:%S")

        resp = self._get(NEWSAPI_BASE, params=params)
        data = resp.json()

        if data.get("status") != "ok":
            raise FetchError(f"NewsAPI error: {data.get('message', 'unknown')}")

        articles: list[RawArticle] = []
        for item in data.get("articles", []):
            url = item.get("url")
            title = item.get("title")
            if not url or not title or url == "https://removed.com":
                continue
            try:
                pub_str = item.get("publishedAt", "")
                pub_dt = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                article = RawArticle(
                    url=url,
                    title=title.strip(),
                    summary=item.get("description"),
                    published_at=pub_dt,
                    source_name=self.config.name,
                    author=item.get("author"),
                    external_id=url,
                    raw_json=json.dumps(item),
                )
                articles.append(article)
            except Exception as exc:
                logger.debug("Skipping NewsAPI item %s: %s", url, exc)

        logger.info(
            "NewsAPI query '%s': fetched %d articles (page %d)",
            query, len(articles), page,
        )
        return articles
