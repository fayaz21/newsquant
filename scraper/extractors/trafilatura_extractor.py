from __future__ import annotations

import logging
from typing import Optional

import httpx
import trafilatura
from bs4 import BeautifulSoup
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import settings

logger = logging.getLogger(__name__)


class ExtractionResult:
    __slots__ = ("body", "title", "author", "language", "method")

    def __init__(
        self,
        body: str,
        title: Optional[str] = None,
        author: Optional[str] = None,
        language: Optional[str] = None,
        method: str = "trafilatura",
    ):
        self.body = body
        self.title = title
        self.author = author
        self.language = language
        self.method = method


class TrafilaturaExtractor:
    """Full-text extractor with fallback chain: trafilatura → RSS content tag → BS4."""

    @retry(
        retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(2),
        reraise=False,
    )
    def _fetch_html(self, url: str) -> Optional[str]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (compatible; FinancialNewsScraper/1.0; "
                "+https://github.com/yourtool)"
            )
        }
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.get(url, headers=headers, follow_redirects=True)
            resp.raise_for_status()
            return resp.text
        return None

    def extract(self, url: str, html: Optional[str] = None) -> Optional[ExtractionResult]:
        """Extract full text. Fetches URL if html not provided."""
        if html is None:
            html = self._fetch_html(url)
        if not html:
            return None

        # Primary: trafilatura
        result = self._try_trafilatura(html)
        if result and len(result.body.split()) >= 50:
            return result

        # Fallback: BS4 paragraph extraction
        result = self._try_bs4(html)
        if result and len(result.body.split()) >= 30:
            return result

        return None

    def _try_trafilatura(self, html: str) -> Optional[ExtractionResult]:
        try:
            text = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                no_fallback=False,
                favor_precision=True,
            )
            if not text:
                return None
            meta = trafilatura.extract_metadata(html)
            return ExtractionResult(
                body=text.strip(),
                title=getattr(meta, "title", None),
                author=getattr(meta, "author", None),
                language=getattr(meta, "language", None),
                method="trafilatura",
            )
        except Exception as exc:
            logger.debug("trafilatura failed: %s", exc)
            return None

    def _try_bs4(self, html: str) -> Optional[ExtractionResult]:
        try:
            soup = BeautifulSoup(html, "lxml")
            # Remove script/style noise
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            paragraphs = soup.find_all("p")
            text = " ".join(p.get_text(separator=" ", strip=True) for p in paragraphs)
            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else None
            if not text:
                return None
            return ExtractionResult(body=text.strip(), title=title, method="bs4")
        except Exception as exc:
            logger.debug("BS4 extraction failed: %s", exc)
            return None
