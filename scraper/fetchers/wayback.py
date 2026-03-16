from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Generator, Optional

from scraper.models.article import RawArticle
from .base import BaseFetcher

logger = logging.getLogger(__name__)

CDX_API = "http://web.archive.org/cdx/search/cdx"


class WaybackFetcher(BaseFetcher):
    """Wayback Machine CDX API — supplemental historical source."""

    def fetch(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        url_cursor: int = 0,
        **kwargs,
    ) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for article, _ in self.iter_articles_resumable(
            from_dt or datetime(2015, 1, 1, tzinfo=timezone.utc),
            to_dt or datetime.now(timezone.utc),
            url_cursor,
        ):
            articles.append(article)
        return articles

    def iter_articles_resumable(
        self,
        from_dt: datetime,
        to_dt: datetime,
        url_cursor: int = 0,
    ) -> Generator[tuple[RawArticle, int], None, None]:
        idx = 0
        for domain in self.config.domains:
            try:
                for article in self._fetch_domain(domain, from_dt, to_dt):
                    if idx < url_cursor:
                        idx += 1
                        continue
                    yield article, idx
                    idx += 1
            except Exception as exc:
                logger.warning("Wayback domain %s failed: %s", domain, exc)

    def _fetch_domain(
        self, domain: str, from_dt: datetime, to_dt: datetime
    ) -> Generator[RawArticle, None, None]:
        params = {
            "url": f"*.{domain}/*",
            "output": "json",
            "from": from_dt.strftime("%Y%m%d"),
            "to": to_dt.strftime("%Y%m%d"),
            "filter": "statuscode:200",
            "fl": "timestamp,original",
            "limit": "10000",
            "collapse": "urlkey",
        }
        try:
            resp = self._get(CDX_API, params=params)
        except Exception as exc:
            logger.warning("CDX API call failed for %s: %s", domain, exc)
            return

        rows = resp.json()
        if not rows or rows[0] == ["timestamp", "original"]:
            rows = rows[1:]  # skip header if present

        for row in rows:
            if len(row) < 2:
                continue
            timestamp_str, original_url = row[0], row[1]
            try:
                pub_dt = datetime.strptime(timestamp_str, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                )
                # Build Wayback URL so we can fetch archived content
                wayback_url = f"https://web.archive.org/web/{timestamp_str}/{original_url}"
                article = RawArticle(
                    url=wayback_url,
                    title=original_url,  # title extracted later by trafilatura
                    published_at=pub_dt,
                    source_name=self.config.name,
                    external_id=f"{timestamp_str}:{original_url}",
                )
                yield article
            except Exception as exc:
                logger.debug("Wayback row skip: %s", exc)
