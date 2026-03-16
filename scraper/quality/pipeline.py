from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional

from config.settings import settings
from scraper.enrichment.language_detector import is_english
from scraper.enrichment.near_duplicate import is_near_duplicate
from scraper.enrichment.ticker_extractor import has_financial_content
from scraper.models.article import RawArticle

logger = logging.getLogger(__name__)

PAYWALL_PHRASES = frozenset([
    "subscribe to read",
    "subscription required",
    "subscribe for full access",
    "this content is only available",
    "premium content",
    "login to read",
    "sign in to read",
    "create a free account",
    "paywall",
    "to continue reading",
    "already a subscriber",
    "unlock this article",
])

# PR Newswire / press-release fluff — reject if body contains ONLY these signals
PR_FLUFF_ONLY_PHRASES = frozenset([
    "for more information, contact",
    "about the company",
    "safe harbor statement",
    "forward-looking statements",
    "non-gaap financial measures",
])


@dataclass
class CheckResult:
    passed: bool
    flag_name: str
    detail: str = ""
    weight: float = 0.0


@dataclass
class QualityResult:
    passed: bool
    quality_score: float
    flags: list[str] = field(default_factory=list)
    is_paywall: bool = False
    is_duplicate: bool = False
    is_near_duplicate: bool = False


class QualityPipeline:
    """Ordered quality checks → composite score.

    Deduplication layers (in order):
      1. URL hash  — exact URL match (fastest)
      2. Content hash — exact body match (syndicated identical copy)
      3. SimHash — near-duplicate body (same story, minor edits)
      4. Title similarity — same headline from different source (soft flag)
    """

    WEIGHTS = {
        "min_word_count":    0.25,
        "completeness_check": 0.25,
        "paywall_detect":    0.20,
        "language_check":    0.15,
        "title_present":     0.10,
        "financial_relevance": 0.05,
    }

    def __init__(
        self,
        db_url_hash_fn: Optional[Callable[[str], bool]] = None,
        db_content_hash_fn: Optional[Callable[[str], bool]] = None,
        db_simhashes_fn: Optional[Callable[[], list[str]]] = None,
    ):
        self._url_exists = db_url_hash_fn or (lambda _: False)
        self._content_exists = db_content_hash_fn or (lambda _: False)
        self._get_simhashes = db_simhashes_fn or (lambda: [])

    def run(
        self,
        raw: RawArticle,
        body: str,
        url_hash: str,
        content_hash: str,
        simhash: Optional[str] = None,
        is_metadata_only: bool = False,
    ) -> QualityResult:
        now = datetime.now(timezone.utc)
        flags: list[str] = []
        is_paywall = False

        # ── Layer 1: URL dedup (fastest — before any other check) ────────────
        if self._url_exists(url_hash):
            return QualityResult(
                passed=False, quality_score=0.0,
                flags=["content_not_duplicate_url"], is_duplicate=True,
            )

        # ── Structural hard-rejects ──────────────────────────────────────────
        completeness = self._check_completeness(raw, body, is_metadata_only)
        if not completeness.passed:
            flags.append(completeness.flag_name)
            return QualityResult(passed=False, quality_score=0.0, flags=flags)

        title_check = self._check_title(raw.title)
        if not title_check.passed:
            flags.append(title_check.flag_name)
            return QualityResult(passed=False, quality_score=0.0, flags=flags)

        date_check = self._check_published_at(raw.published_at, now)
        if not date_check.passed:
            flags.append(date_check.flag_name)
            return QualityResult(passed=False, quality_score=0.0, flags=flags)

        # ── Content quality checks ───────────────────────────────────────────
        paywall_check = self._check_paywall(body)
        if not paywall_check.passed:
            flags.append(paywall_check.flag_name)
            is_paywall = True

        if not is_metadata_only:
            word_count = len(body.split())
            wc_check = self._check_word_count(word_count)
            if not wc_check.passed:
                flags.append(wc_check.flag_name)
                if is_paywall:
                    return QualityResult(passed=False, quality_score=0.0, flags=flags, is_paywall=True)
                return QualityResult(passed=False, quality_score=0.0, flags=flags)

            if word_count > 50_000:
                flags.append("max_word_count_exceeded")

            lang_check = self._check_language(body)
            if not lang_check.passed:
                flags.append(lang_check.flag_name)
                return QualityResult(passed=False, quality_score=0.0, flags=flags)

        # ── Layer 2: Content hash dedup ──────────────────────────────────────
        if content_hash and self._content_exists(content_hash):
            return QualityResult(
                passed=False, quality_score=0.0,
                flags=["content_not_duplicate_body"], is_duplicate=True,
            )

        # ── Layer 3: SimHash near-duplicate ──────────────────────────────────
        is_near_dup = False
        if simhash and not is_metadata_only:
            recent_hashes = self._get_simhashes()
            if is_near_duplicate(simhash, recent_hashes):
                flags.append("near_duplicate_simhash")
                is_near_dup = True
                # Near-dups are stored (not rejected) — flagged for downstream filtering
                # This preserves cross-source coverage while marking the signal

        # ── Soft checks ──────────────────────────────────────────────────────
        financial_check = self._check_financial_relevance(raw.title, body)
        if not financial_check.passed:
            flags.append(financial_check.flag_name)

        score = self._compute_score(
            completeness_ok=True,
            title_ok=True,
            paywall_ok=not is_paywall,
            word_count_ok=not is_metadata_only,  # metadata-only gets full word_count credit
            language_ok=not is_metadata_only,
            financial_ok=financial_check.passed,
            is_metadata_only=is_metadata_only,
        )

        return QualityResult(
            passed=True,
            quality_score=round(score, 4),
            flags=flags,
            is_paywall=is_paywall,
            is_duplicate=False,
            is_near_duplicate=is_near_dup,
        )

    # ── Individual checks ────────────────────────────────────────────────────

    def _check_completeness(self, raw: RawArticle, body: str, metadata_only: bool) -> CheckResult:
        missing = []
        if not raw.title:
            missing.append("title")
        if not body and not metadata_only:
            missing.append("body")
        if not raw.url:
            missing.append("url")
        if not raw.published_at:
            missing.append("published_at")
        if not raw.source_name:
            missing.append("source_name")
        passed = len(missing) == 0
        return CheckResult(
            passed=passed,
            flag_name="completeness_check",
            detail=f"missing: {missing}" if missing else "",
            weight=self.WEIGHTS["completeness_check"],
        )

    def _check_title(self, title: str) -> CheckResult:
        passed = bool(title) and len(title.strip()) > 10
        return CheckResult(
            passed=passed,
            flag_name="title_present",
            detail=f"title length={len(title.strip())}",
            weight=self.WEIGHTS["title_present"],
        )

    def _check_published_at(self, pub_dt: datetime, now: datetime) -> CheckResult:
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        future = pub_dt > now + timedelta(hours=1)
        too_old = pub_dt < now - timedelta(days=365 * 6)
        passed = not future and not too_old
        detail = "future date" if future else ("too old (>6yr)" if too_old else "")
        return CheckResult(passed=passed, flag_name="published_at_valid", detail=detail)

    def _check_paywall(self, body: str) -> CheckResult:
        lower = body.lower()
        for phrase in PAYWALL_PHRASES:
            if phrase in lower:
                return CheckResult(
                    passed=False, flag_name="paywall_detect", detail=phrase,
                    weight=self.WEIGHTS["paywall_detect"],
                )
        return CheckResult(passed=True, flag_name="paywall_detect",
                           weight=self.WEIGHTS["paywall_detect"])

    def _check_word_count(self, word_count: int) -> CheckResult:
        passed = word_count >= settings.min_word_count
        return CheckResult(
            passed=passed,
            flag_name="min_word_count",
            detail=f"word_count={word_count}, min={settings.min_word_count}",
            weight=self.WEIGHTS["min_word_count"],
        )

    def _check_language(self, body: str) -> CheckResult:
        ok, lang, conf = is_english(body, settings.language_confidence_threshold)
        return CheckResult(
            passed=ok,
            flag_name="language_check",
            detail=f"lang={lang}, confidence={conf:.2f}",
            weight=self.WEIGHTS["language_check"],
        )

    def _check_financial_relevance(self, title: str, body: str) -> CheckResult:
        passed = has_financial_content(body, title)
        return CheckResult(
            passed=passed,
            flag_name="financial_relevance",
            weight=self.WEIGHTS["financial_relevance"],
        )

    def _compute_score(
        self,
        completeness_ok: bool,
        title_ok: bool,
        paywall_ok: bool,
        word_count_ok: bool,
        language_ok: bool,
        financial_ok: bool,
        is_metadata_only: bool = False,
    ) -> float:
        score = 0.0
        if word_count_ok or is_metadata_only:
            score += self.WEIGHTS["min_word_count"]
        if completeness_ok:
            score += self.WEIGHTS["completeness_check"]
        if paywall_ok:
            score += self.WEIGHTS["paywall_detect"]
        if language_ok or is_metadata_only:
            score += self.WEIGHTS["language_check"]
        if title_ok:
            score += self.WEIGHTS["title_present"]
        if financial_ok:
            score += self.WEIGHTS["financial_relevance"]
        # Metadata-only articles are capped at 0.75 to distinguish from full-text
        if is_metadata_only:
            score = min(score, 0.75)
        return score
