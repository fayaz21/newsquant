from __future__ import annotations

import logging
import re
from functools import lru_cache
from pathlib import Path
logger = logging.getLogger(__name__)

# Regex patterns for common ticker formats
_DOLLAR_TICKER = re.compile(r"\$([A-Z]{1,5})\b")
_EXCHANGE_TICKER = re.compile(
    r"\b(?:NYSE|NASDAQ|AMEX|TSX|LSE):\s*([A-Z]{1,5})\b"
)

# Common financial keywords for relevance checks
FINANCIAL_KEYWORDS = frozenset([
    "stock", "share", "equity", "market", "trading", "investor", "earnings",
    "revenue", "profit", "loss", "ipo", "merger", "acquisition", "dividend",
    "fed", "federal reserve", "interest rate", "inflation", "gdp", "economy",
    "s&p", "dow", "nasdaq", "bond", "yield", "hedge fund", "portfolio",
    "analyst", "upgrade", "downgrade", "buy", "sell", "hold", "target price",
    "quarterly", "guidance", "outlook", "fiscal", "quarterly results",
])


@lru_cache(maxsize=1)
def _load_sp500_tickers() -> frozenset[str]:
    """Load S&P 500 tickers from bundled CSV (if available)."""
    csv_path = Path(__file__).parent / "data" / "sp500_tickers.csv"
    if csv_path.exists():
        tickers = set()
        with open(csv_path) as f:
            for line in f:
                ticker = line.strip().upper()
                if ticker:
                    tickers.add(ticker)
        return frozenset(tickers)
    return frozenset()


def extract_tickers(text: str, use_spacy: bool = False) -> list[str]:
    """Extract ticker symbols using regex patterns + optional spaCy NER."""
    tickers: set[str] = set()

    # Pattern 1: $AAPL style
    for match in _DOLLAR_TICKER.finditer(text):
        tickers.add(match.group(1).upper())

    # Pattern 2: NYSE: JPM style
    for match in _EXCHANGE_TICKER.finditer(text):
        tickers.add(match.group(1).upper())

    if use_spacy:
        tickers.update(_spacy_extract(text))

    # Filter to known tickers if CSV is loaded
    known = _load_sp500_tickers()
    if known:
        tickers = tickers & known

    return sorted(tickers)


def _spacy_extract(text: str) -> list[str]:
    try:
        import spacy
        nlp = _get_spacy_model()
        if nlp is None:
            return []
        doc = nlp(text[:5000])  # cap for speed
        results = []
        for ent in doc.ents:
            if ent.label_ == "ORG":
                # Simple heuristic: all-caps short tokens could be tickers
                token = ent.text.strip().upper()
                if re.match(r"^[A-Z]{1,5}$", token):
                    results.append(token)
        return results
    except Exception as exc:
        logger.debug("spaCy extraction failed: %s", exc)
        return []


@lru_cache(maxsize=1)
def _get_spacy_model():
    try:
        import spacy
        return spacy.load("en_core_web_sm")
    except Exception:
        logger.debug("spaCy model not loaded — install 'en_core_web_sm'")
        return None


def has_financial_content(text: str, title: str = "") -> bool:
    """True if text or title contains financial keywords."""
    combined = title + " " + text
    combined_lower = combined.lower()
    for keyword in FINANCIAL_KEYWORDS:
        if keyword in combined_lower:
            return True
    # Check for ticker patterns on original-case text (regex requires UPPERCASE)
    if _DOLLAR_TICKER.search(combined) or _EXCHANGE_TICKER.search(combined):
        return True
    return False
