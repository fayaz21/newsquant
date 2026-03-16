# finews

**The financial news data library for Python.**

`pip install finews`

[![PyPI](https://img.shields.io/pypi/v/finews)](https://pypi.org/project/finews/)
[![Python](https://img.shields.io/pypi/pyversions/finews)](https://pypi.org/project/finews/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

Fetch, extract, and score financial news articles from 13 sources in one call.
Returns clean `Article` objects with full text, extracted tickers, and a quality
score — no database required.

**What gap does this fill?**
Existing options are either single-source (`newsapi-python`, `feedparser`),
return no article body (`yfinance`), or have no quality layer at all. finews
covers multiple sources, runs full-text extraction via
[trafilatura](https://github.com/adbar/trafilatura), deduplicates across
sources, and scores every article on a 0–1 quality scale.

---

## Install

```bash
pip install finews
```

Python 3.9+ required. For PostgreSQL persistence add the optional extra:

```bash
pip install "finews[postgres]"
```

---

## Quickstart

```python
from finews import Scraper

scraper = Scraper(sources=["yahoofinance", "cnbc"])
articles = scraper.fetch(days_back=1)

for a in articles:
    print(a.title, a.tickers, a.quality_score)
```

---

## Examples

### Filter by ticker

```python
scraper = Scraper(sources=["yahoofinance", "benzinga", "cnbc"])
articles = scraper.fetch(tickers=["AAPL", "MSFT"], days_back=7)

for a in articles:
    print(f"[{', '.join(a.tickers)}] {a.title}")
    print(f"  source={a.source_name}  quality={a.quality_score:.2f}  words={a.word_count}")
```

### Use API sources

```python
scraper = Scraper(
    sources=["newsapi", "finnhub"],
    newsapi_key="YOUR_KEY",
    finnhub_api_key="YOUR_KEY",
)
articles = scraper.fetch(tickers=["NVDA"], days_back=3, min_quality=0.8)
```

API keys can also be set via environment variables — see [Configuration](#configuration).

### Persist to a database

```python
# SQLite
articles = scraper.fetch(save_to="sqlite:///./financial_news.db")

# PostgreSQL
articles = scraper.fetch(save_to="postgresql://user:pw@localhost/mydb")
```

Tables are created automatically if they don't exist. `fetch()` always
returns the `Article` list regardless.

### Custom source

Subclass `BaseFetcher` to add any source the pipeline doesn't cover.

```python
from finews import Scraper, BaseFetcher, SourceConfig
from scraper.models.article import RawArticle
from datetime import datetime, timezone

class MySource(BaseFetcher):
    def __init__(self, api_key: str):
        super().__init__(SourceConfig(name="my_source", type="api", rate_limit_rps=2.0))
        self._api_key = api_key

    def fetch(self, from_dt=None, to_dt=None, ticker=None, **kwargs) -> list[RawArticle]:
        resp = self._get(
            "https://api.example.com/news",
            params={"key": self._api_key, "symbol": ticker},
        )
        return [
            RawArticle(
                url=item["url"],
                title=item["title"],
                published_at=datetime.fromisoformat(item["published_at"]),
                source_name=self.config.name,
                summary=item.get("summary"),
            )
            for item in resp.json()["articles"]
        ]

# Mix custom and built-in sources freely
scraper = Scraper(sources=[MySource(api_key="secret"), "cnbc", "yahoofinance"])
articles = scraper.fetch(tickers=["AAPL"])
```

`BaseFetcher` provides `_get()` and `_post()` with automatic rate limiting and
exponential-backoff retries. The rest of the pipeline (extraction, dedup,
quality scoring, ticker extraction) runs automatically.

---

## Built-in sources

| Name | Type | Full text | Notes |
|---|---|---|---|
| `yahoofinance` | RSS | ✓ | |
| `cnbc` | RSS | ✓ | |
| `motleyfool` | RSS | ✓ | |
| `benzinga` | RSS | ✓ | |
| `businessinsider` | RSS | ✓ | |
| `fortune` | RSS | ✓ | |
| `prnewswire` | RSS | ✓ | Financial press releases only |
| `bloomberg` | RSS | — | Title + summary (paywalled) |
| `wsj` | RSS | — | Title + summary (paywalled) |
| `ft` | RSS | — | Title + summary (paywalled) |
| `seekingalpha` | RSS | — | Title + summary (paywalled) |
| `newsapi` | API | ✓ | [Key required](https://newsapi.org/register) |
| `finnhub` | API | ✓ | [Key required](https://finnhub.io/register) |

`Scraper()` with no `sources` argument defaults to all RSS sources.

---

## The `Article` object

Every item returned by `fetch()` is a Pydantic model with these fields:

| Field | Type | Description |
|---|---|---|
| `title` | `str` | Article headline |
| `body` | `str` | Full extracted text |
| `summary` | `str` | Lead paragraph or RSS summary |
| `url` | `str` | Canonical URL |
| `source_name` | `str` | Source identifier (e.g. `"cnbc_rss"`) |
| `author` | `str \| None` | Byline if available |
| `published_at` | `datetime` | Publication time (UTC) |
| `tickers` | `list[str]` | Extracted ticker symbols |
| `quality_score` | `float` | 0–1 composite quality score |
| `quality_flags` | `list[str]` | Flags that reduced the score |
| `word_count` | `int` | Body word count |
| `language` | `str` | Detected language code |
| `is_paywall` | `bool` | Paywall detected |
| `is_duplicate` | `bool` | Exact duplicate (URL or body hash) |
| `is_near_duplicate` | `bool` | Near-duplicate (SimHash) |
| `is_metadata_only` | `bool` | Full-text extraction skipped |

---

## Configuration

Set these in a `.env` file or as environment variables. Only the API keys for
sources you actually use are required.

```bash
# Required for Scraper(sources=["newsapi"])
NEWSAPI_KEY=

# Required for Scraper(sources=["finnhub"])
FINNHUB_API_KEY=

# Optional — defaults shown
DATABASE_URL=sqlite:///./financial_news.db
MIN_WORD_COUNT=150
LANGUAGE_CONFIDENCE_THRESHOLD=0.95
REQUEST_TIMEOUT_SECONDS=30
MAX_RETRIES=3
LOG_LEVEL=INFO
```

Copy `.env.example` to get started:

```bash
cp .env.example .env
```

---

## CLI

A command-line interface ships alongside the Python API for ops tasks:

```bash
# One-time setup
scraper db init

# Run sources
scraper scrape --all
scraper scrape --source cnbc_rss

# Historical backfill (GDELT and Wayback Machine)
scraper backfill --source gdelt --start 2020-01-01 --end 2025-01-01 --workers 4

# Query stored articles
scraper query --ticker AAPL --min-quality 0.8 --format csv

# Real-time daemon
scraper scheduler start --daemon
```

---

## Contributing

Bug reports and pull requests are welcome. For major changes, open an issue
first to discuss what you'd like to change.

```bash
git clone https://github.com/your-username/finews
cd finews
pip install -e ".[dev]"
pytest
```

---

## License

[MIT](LICENSE)
