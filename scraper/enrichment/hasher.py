from __future__ import annotations

import hashlib
import re
from urllib.parse import urlparse, urlunparse


def normalize_url(url: str) -> str:
    """Strip tracking params, fragments, and normalize scheme for stable hashing."""
    parsed = urlparse(url.strip())
    # Drop fragment
    normalized = parsed._replace(fragment="")
    # Drop common tracking params
    _TRACKING = frozenset(
        ["utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
         "ref", "src", "source", "fbclid", "gclid"]
    )
    if normalized.query:
        parts = []
        for part in normalized.query.split("&"):
            key = part.split("=")[0]
            if key not in _TRACKING:
                parts.append(part)
        normalized = normalized._replace(query="&".join(sorted(parts)))
    return urlunparse(normalized).lower()


def url_hash(url: str) -> str:
    return hashlib.sha256(normalize_url(url).encode()).hexdigest()


def content_hash(text: str) -> str:
    # Normalize whitespace before hashing to catch near-duplicates
    normalized = re.sub(r"\s+", " ", text.strip().lower())
    return hashlib.sha256(normalized.encode()).hexdigest()
