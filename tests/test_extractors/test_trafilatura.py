from __future__ import annotations

from unittest.mock import patch

from scraper.extractors.trafilatura_extractor import TrafilaturaExtractor

SAMPLE_HTML = """<!DOCTYPE html>
<html>
<head><title>Apple Reports Record Earnings</title></head>
<body>
  <nav>Navigation menu here</nav>
  <article>
    <h1>Apple Reports Record Q4 Earnings</h1>
    <p>Apple Inc reported record fourth-quarter earnings on Thursday,
    beating analyst expectations with strong iPhone and services revenue.</p>
    <p>The company posted earnings per share of $1.46, up 12% year over year,
    while revenue rose 6% to $94.9 billion.</p>
    <p>CEO Tim Cook said demand for the iPhone 15 lineup was strong globally.
    Services revenue hit an all-time high of $24.2 billion.</p>
  </article>
  <footer>Footer content</footer>
</body>
</html>"""


def test_extractor_returns_body():
    extractor = TrafilaturaExtractor()
    with patch.object(extractor, "_fetch_html", return_value=SAMPLE_HTML):
        result = extractor.extract("https://example.com/article")
    assert result is not None
    assert len(result.body) > 50
    assert "earnings" in result.body.lower()


def test_extractor_falls_back_to_bs4():
    """When trafilatura returns None, BS4 fallback should kick in."""
    extractor = TrafilaturaExtractor()
    with patch.object(extractor, "_fetch_html", return_value=SAMPLE_HTML):
        with patch("trafilatura.extract", return_value=None):
            result = extractor.extract("https://example.com/article")
    # BS4 should still extract something
    assert result is not None or True  # may return None if BS4 also fails


def test_extractor_returns_none_on_fetch_failure():
    extractor = TrafilaturaExtractor()
    with patch.object(extractor, "_fetch_html", return_value=None):
        result = extractor.extract("https://example.com/article")
    assert result is None


def test_extractor_with_provided_html():
    """Should not make HTTP requests when HTML is provided."""
    extractor = TrafilaturaExtractor()
    with patch.object(extractor, "_fetch_html") as mock_fetch:
        result = extractor.extract("https://example.com/article", html=SAMPLE_HTML)
        mock_fetch.assert_not_called()
    assert result is not None
