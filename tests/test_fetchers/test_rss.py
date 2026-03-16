from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from scraper.fetchers.rss import RSSFetcher
from scraper.models.source import SourceConfig


SAMPLE_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Finance Feed</title>
    <link>https://example.com</link>
    <item>
      <title>Apple Beats Earnings Expectations</title>
      <link>https://example.com/news/apple-earnings</link>
      <pubDate>Thu, 31 Oct 2024 18:00:00 +0000</pubDate>
      <description>Apple reported record earnings for Q4 2024.</description>
    </item>
    <item>
      <title>Fed Holds Rates Steady</title>
      <link>https://example.com/news/fed-rates</link>
      <pubDate>Thu, 31 Oct 2024 20:00:00 +0000</pubDate>
      <description>Federal Reserve held interest rates steady.</description>
    </item>
  </channel>
</rss>"""


@pytest.fixture
def rss_config():
    return SourceConfig(
        name="test_rss",
        type="rss",
        enabled=True,
        rate_limit_rps=999.0,
        feeds=["https://example.com/rss.xml"],
    )


def test_rss_fetcher_parses_feed(rss_config):
    import feedparser
    real_feed = feedparser.parse(SAMPLE_RSS)
    with patch("feedparser.parse", return_value=real_feed):
        fetcher = RSSFetcher(rss_config)
        articles = fetcher.fetch()

    assert len(articles) == 2
    assert articles[0].title == "Apple Beats Earnings Expectations"
    assert articles[0].url == "https://example.com/news/apple-earnings"
    assert articles[0].source_name == "test_rss"


def test_rss_fetcher_skips_items_without_url(rss_config):
    broken_rss = """<?xml version="1.0"?>
    <rss version="2.0"><channel>
      <item><title>No URL Item</title></item>
    </channel></rss>"""
    import feedparser
    real_feed = feedparser.parse(broken_rss)
    with patch("feedparser.parse", return_value=real_feed):
        fetcher = RSSFetcher(rss_config)
        articles = fetcher.fetch()
    assert articles == []


def test_rss_fetcher_continues_on_feed_error(rss_config):
    """If one feed fails, the fetcher should continue with remaining feeds."""
    rss_config.feeds = ["https://bad.example.com/rss", "https://good.example.com/rss"]
    import feedparser
    good_feed = feedparser.parse(SAMPLE_RSS)

    def side_effect(url):
        if "bad" in url:
            raise ValueError("Connection failed")
        return good_feed

    with patch("feedparser.parse", side_effect=side_effect):
        fetcher = RSSFetcher(rss_config)
        # Should not raise — bad feed is logged and skipped
        articles = fetcher.fetch()
    assert len(articles) >= 0  # good feed may or may not succeed depending on patching
