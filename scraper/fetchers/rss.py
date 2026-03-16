from __future__ import annotations

import logging
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional

import feedparser

from scraper.models.article import RawArticle
from scraper.models.source import SourceConfig
from .base import BaseFetcher

logger = logging.getLogger(__name__)


def _parse_date(entry) -> datetime:
    """Try multiple date fields; fall back to now (UTC)."""
    for field in ("published_parsed", "updated_parsed"):
        val = getattr(entry, field, None)
        if val:
            try:
                return datetime(*val[:6], tzinfo=timezone.utc)
            except Exception:
                pass
    for field in ("published", "updated"):
        val = getattr(entry, field, None)
        if val:
            try:
                return parsedate_to_datetime(val).astimezone(timezone.utc)
            except Exception:
                pass
    return datetime.now(timezone.utc)


def _entry_summary(entry) -> Optional[str]:
    for field in ("summary", "description", "content"):
        val = getattr(entry, field, None)
        if val:
            if isinstance(val, list):
                return val[0].get("value", "")
            return str(val)
    return None


class RSSFetcher(BaseFetcher):
    def __init__(self, config: SourceConfig):
        super().__init__(config)

    def fetch(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        ticker: Optional[str] = None,
        **kwargs,
    ) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for feed_url in self.config.feeds:
            try:
                articles.extend(self._fetch_feed(feed_url))
            except Exception as exc:
                logger.warning("RSS feed %s failed: %s", feed_url, exc)
        return articles

    def _fetch_feed(self, feed_url: str) -> list[RawArticle]:
        self._rate_limit()
        feed = feedparser.parse(feed_url)
        if feed.bozo and not feed.entries:
            raise ValueError(f"Malformed feed: {feed_url}")

        articles: list[RawArticle] = []
        for entry in feed.entries:
            url = getattr(entry, "link", None)
            title = getattr(entry, "title", None)
            if not url or not title:
                continue
            try:
                article = RawArticle(
                    url=url,
                    title=title.strip(),
                    summary=_entry_summary(entry),
                    published_at=_parse_date(entry),
                    source_name=self.config.name,
                    author=getattr(entry, "author", None),
                    external_id=getattr(entry, "id", url),
                )
                articles.append(article)
            except Exception as exc:
                logger.debug("Skipping RSS entry %s: %s", url, exc)

        logger.info("RSS %s: fetched %d items from %s", self.config.name, len(articles), feed_url)
        return articles
