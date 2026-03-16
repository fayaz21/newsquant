from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scraper.models.article import RawArticle
from scraper.quality.pipeline import QualityPipeline


def _make_raw(
    title="Apple Reports Record Earnings for Q4",
    url="https://example.com/news/apple-q4",
    source_name="test",
    published_at=None,
) -> RawArticle:
    return RawArticle(
        url=url,
        title=title,
        published_at=published_at or datetime(2024, 10, 31, tzinfo=timezone.utc),
        source_name=source_name,
    )


GOOD_BODY = (
    "Apple Inc reported record fourth-quarter earnings on Thursday, "
    "beating analyst expectations with strong iPhone and services revenue. "
    "The company posted earnings per share of $1.46, up 12% year over year, "
    "while revenue rose 6% to $94.9 billion. CEO Tim Cook said demand for "
    "the iPhone 15 lineup was strong globally. Services revenue hit an "
    "all-time high of $24.2 billion. Investors reacted positively, sending "
    "$AAPL stock up 2% in after-hours trading. Analysts at Goldman Sachs "
    "maintained their buy rating. The stock market reacted positively. "
) * 5  # ensure > 150 words


def test_good_article_passes():
    pipeline = QualityPipeline()
    raw = _make_raw()
    from scraper.enrichment.hasher import content_hash, url_hash
    from scraper.enrichment.near_duplicate import compute_simhash
    result = pipeline.run(raw, GOOD_BODY, url_hash(raw.url), content_hash(GOOD_BODY),
                          simhash=compute_simhash(GOOD_BODY))
    assert result.passed is True
    assert result.quality_score > 0.7
    assert "min_word_count" not in result.flags


def test_short_body_fails():
    pipeline = QualityPipeline()
    raw = _make_raw()
    short_body = "Too short."
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, short_body, url_hash(raw.url), content_hash(short_body))
    assert result.passed is False
    assert "min_word_count" in result.flags


def test_missing_title_fails():
    pipeline = QualityPipeline()
    raw = _make_raw(title="Hi")  # too short
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, GOOD_BODY, url_hash(raw.url), content_hash(GOOD_BODY))
    assert result.passed is False
    assert "title_present" in result.flags


def test_paywall_body_sets_flag():
    pipeline = QualityPipeline()
    raw = _make_raw()
    paywall_body = "subscribe to read this premium content. " * 5
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, paywall_body, url_hash(raw.url), content_hash(paywall_body))
    assert result.is_paywall is True


def test_url_dedup_returns_duplicate():
    seen: set[str] = set()

    def url_exists(h: str) -> bool:
        if h in seen:
            return True
        seen.add(h)
        return False

    pipeline = QualityPipeline(db_url_hash_fn=url_exists)
    raw = _make_raw()
    from scraper.enrichment.hasher import content_hash, url_hash
    uh = url_hash(raw.url)
    ch = content_hash(GOOD_BODY)

    # First pass — should store
    result1 = pipeline.run(raw, GOOD_BODY, uh, ch)
    assert result1.passed is True

    # Second pass — same URL hash already in "DB"
    result2 = pipeline.run(raw, GOOD_BODY, uh, ch)
    assert result2.passed is False
    assert result2.is_duplicate is True


def test_future_date_rejected():
    pipeline = QualityPipeline()
    future = datetime(2099, 1, 1, tzinfo=timezone.utc)
    raw = _make_raw(published_at=future)
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, GOOD_BODY, url_hash(raw.url), content_hash(GOOD_BODY))
    assert result.passed is False
    assert "published_at_valid" in result.flags


def test_quality_score_components():
    pipeline = QualityPipeline()
    raw = _make_raw()
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, GOOD_BODY, url_hash(raw.url), content_hash(GOOD_BODY))
    assert 0.0 < result.quality_score <= 1.0


def test_near_duplicate_flagged():
    """Same body submitted twice should flag the second as near-duplicate."""
    from scraper.enrichment.near_duplicate import compute_simhash
    from scraper.enrichment.hasher import content_hash, url_hash

    stored_hashes: list[str] = []
    pipeline = QualityPipeline(db_simhashes_fn=lambda: stored_hashes)

    raw1 = _make_raw(url="https://example.com/story-a")
    sh = compute_simhash(GOOD_BODY)

    result1 = pipeline.run(raw1, GOOD_BODY, url_hash(raw1.url), content_hash(GOOD_BODY), simhash=sh)
    assert result1.passed is True
    stored_hashes.append(sh)  # simulate storing first article's hash

    raw2 = _make_raw(url="https://example.com/story-b")
    result2 = pipeline.run(raw2, GOOD_BODY, url_hash(raw2.url), content_hash(GOOD_BODY + " extra"), simhash=sh)
    assert result2.passed is True  # still stored, just flagged
    assert result2.is_near_duplicate is True
    assert "near_duplicate_simhash" in result2.flags


def test_metadata_only_article_capped_score():
    """Metadata-only articles should be stored but capped at 0.75 quality."""
    pipeline = QualityPipeline()
    raw = _make_raw()
    from scraper.enrichment.hasher import content_hash, url_hash
    result = pipeline.run(raw, raw.title, url_hash(raw.url), content_hash(raw.title),
                          is_metadata_only=True)
    assert result.passed is True
    assert result.quality_score <= 0.75
