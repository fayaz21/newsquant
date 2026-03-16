from __future__ import annotations

"""Domain-level fast-fail and title similarity utilities."""

from urllib.parse import urlparse

# Known hard-paywall domains — skip full-text fetch immediately, no retries
PAYWALL_DOMAINS: frozenset[str] = frozenset([
    "wsj.com",
    "barrons.com",
    "ft.com",
    "economist.com",
    "bloomberg.com",
    "marketwatch.com",
    "investors.com",
    "theatlantic.com",
    "nytimes.com",
    "washingtonpost.com",
])

# Domains that block scrapers (403/401) but are not paywalled — metadata still OK
SCRAPER_BLOCKED_DOMAINS: frozenset[str] = frozenset([
    "investing.com",
    "seekingalpha.com",
    "morningstar.com",
    "zacks.com",
])


def get_domain(url: str) -> str:
    """Return root domain (e.g. 'wsj.com') from any URL."""
    try:
        host = urlparse(url).hostname or ""
        if host.startswith("www."):
            host = host[4:]
        parts = host.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host
    except Exception:
        return ""


def is_paywalled_domain(url: str) -> bool:
    return get_domain(url) in PAYWALL_DOMAINS


def is_scraper_blocked(url: str) -> bool:
    return get_domain(url) in SCRAPER_BLOCKED_DOMAINS


def title_similarity(a: str, b: str) -> float:
    """Jaccard similarity on word sets (lowercased). 1.0 = identical."""
    set_a = set(a.lower().split())
    set_b = set(b.lower().split())
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)
