# Changelog

All notable changes to this project will be documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project uses [semantic versioning](https://semver.org/).

---

## [0.1.0] — 2026-03-16

Initial public release.

### Added

**Public Python API (`finews`)**
- `Scraper` class — single entry point for fetching enriched articles
- `Scraper.fetch()` — returns `list[Article]`, no database required
  - `tickers` — post-filter by extracted ticker symbols
  - `days_back` / `from_dt` / `to_dt` — date range control
  - `min_quality` — discard articles below a quality threshold
  - `limit` — cap result count
  - `save_to` — optional SQLAlchemy URL for persistent storage
- `BaseFetcher` — abstract base class for custom sources, with built-in HTTP helpers (`_get`, `_post`, `_rate_limit`)
- `SourceConfig` — Pydantic config model for describing a source
- `FetchError` — exception class for unrecoverable fetch failures

**Built-in sources (13 total)**
- Full-text RSS: `yahoofinance`, `cnbc`, `motleyfool`, `benzinga`, `businessinsider`, `fortune`, `prnewswire`
- Metadata-only RSS (paywalled): `bloomberg`, `wsj`, `ft`, `seekingalpha`
- API (key required): `newsapi`, `finnhub`

**Pipeline internals**
- Multi-stage quality scoring (0–1): completeness, word count, paywall detection, language check, financial relevance
- Three-layer deduplication: URL hash → content hash → SimHash near-duplicate
- Full-text extraction via [trafilatura](https://github.com/adbar/trafilatura)
- Ticker and company extraction using spaCy NER + curated symbol lists
- Language detection with configurable confidence threshold

**CLI (`scraper`)**
- `scraper db init` / `scraper db migrate` / `scraper db stats`
- `scraper scrape --all` / `--source <name>`
- `scraper backfill --source <name> --start <date> --end <date> --workers <n>`
- `scraper query` with ticker, date, quality, and format filters
- `scraper scheduler start --daemon`

**Storage**
- SQLite (default, WAL mode) and PostgreSQL via `DATABASE_URL`
- Alembic-managed schema migrations
- Optional persistence from the Python API via `save_to=` parameter

**Infrastructure**
- GitHub Actions CI: test on push/PR across Python 3.9–3.12, ruff lint, mypy type check
- GitHub Actions publish: build and push to PyPI on version tag
- MIT license
