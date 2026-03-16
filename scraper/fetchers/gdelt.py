from __future__ import annotations

import csv
import io
import logging
import zipfile
from datetime import datetime, timezone
from typing import Generator, Optional

from scraper.models.article import RawArticle
from scraper.models.source import SourceConfig
from .base import BaseFetcher

logger = logging.getLogger(__name__)

# GDELT 2.0 master file list — updated every 15 minutes
GDELT_MASTER_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GDELT_MASTER_LAST30 = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
GKG_SUFFIX = ".gkg.csv.zip"


class GDELTFetcher(BaseFetcher):
    """GDELT 2.0 GKG CSV downloader — primary backfill source."""

    def __init__(self, config: SourceConfig):
        super().__init__(config)
        self._financial_themes = set(config.financial_themes)

    def fetch(
        self,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None,
        url_cursor: int = 0,
        **kwargs,
    ) -> list[RawArticle]:
        articles: list[RawArticle] = []
        for raw in self._iter_articles(from_dt, to_dt, url_cursor):
            articles.append(raw)
        return articles

    def iter_articles_resumable(
        self,
        from_dt: datetime,
        to_dt: datetime,
        url_cursor: int = 0,
    ) -> Generator[tuple[RawArticle, int], None, None]:
        """Yield (article, global_url_index) so caller can save cursor."""
        idx = 0
        for file_url in self._iter_gkg_file_urls(from_dt, to_dt):
            try:
                rows = self._download_gkg_file(file_url)
            except Exception as exc:
                logger.warning("GDELT file %s failed: %s", file_url, exc)
                continue
            for row_url, row_title, row_dt, row_themes in rows:
                if idx < url_cursor:
                    idx += 1
                    continue
                if not self._is_financial(row_themes):
                    idx += 1
                    continue
                try:
                    article = RawArticle(
                        url=row_url,
                        title=row_title or row_url,
                        published_at=row_dt,
                        source_name=self.config.name,
                        external_id=f"gdelt:{idx}",
                    )
                    yield article, idx
                except Exception as exc:
                    logger.debug("GDELT row %d skip: %s", idx, exc)
                idx += 1

    def _iter_articles(
        self,
        from_dt: Optional[datetime],
        to_dt: Optional[datetime],
        url_cursor: int,
    ) -> Generator[RawArticle, None, None]:
        for article, _ in self.iter_articles_resumable(
            from_dt or datetime(2015, 1, 1, tzinfo=timezone.utc),
            to_dt or datetime.now(timezone.utc),
            url_cursor,
        ):
            yield article

    def _iter_gkg_file_urls(
        self, from_dt: datetime, to_dt: datetime
    ) -> Generator[str, None, None]:
        try:
            resp = self._get(GDELT_MASTER_URL)
            lines = resp.text.splitlines()
        except Exception as exc:
            logger.error("Failed to fetch GDELT master list: %s", exc)
            return

        for line in lines:
            parts = line.split()
            if len(parts) < 3:
                continue
            file_url = parts[2]
            if not file_url.endswith(GKG_SUFFIX):
                continue
            # Parse timestamp from filename: YYYYMMDDHHMMSS
            fname = file_url.split("/")[-1]
            ts_str = fname[:14]
            try:
                file_dt = datetime.strptime(ts_str, "%Y%m%d%H%M%S").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            if from_dt <= file_dt <= to_dt:
                yield file_url

    def _download_gkg_file(
        self, file_url: str
    ) -> list[tuple[str, str, datetime, str]]:
        resp = self._get(file_url)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            csv_name = zf.namelist()[0]
            with zf.open(csv_name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="utf-8", errors="replace"), delimiter="\t")
                rows = []
                for row in reader:
                    if len(row) < 5:
                        continue
                    date_str = row[1] if len(row) > 1 else ""
                    source_url = row[4] if len(row) > 4 else ""
                    themes = row[7] if len(row) > 7 else ""
                    title = row[10] if len(row) > 10 else ""
                    if not source_url:
                        continue
                    try:
                        dt = datetime.strptime(date_str, "%Y%m%d%H%M%S").replace(
                            tzinfo=timezone.utc
                        )
                    except ValueError:
                        dt = datetime.now(timezone.utc)
                    rows.append((source_url, title, dt, themes))
                return rows

    def _is_financial(self, themes: str) -> bool:
        if not themes:
            return False
        for theme_prefix in self._financial_themes:
            if theme_prefix.upper() in themes.upper():
                return True
        return False
