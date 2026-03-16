"""Near-duplicate detection using SimHash.

Strategy:
  1. Compute a 64-bit SimHash of the article body (normalised tokens).
  2. Store the hash in the DB.
  3. On insert, compare against recent hashes using Hamming distance ≤ threshold.
     Articles that differ in ≤ 3 bits (out of 64) are near-duplicates.

This catches:
  - Same story rephrased slightly across sources
  - Syndicated wire copy with minor edits
  - Boilerplate-heavy press releases cloned across PR sites
"""

from __future__ import annotations

import re

# Hamming distance threshold — ≤10 bits different (out of 64) → near-duplicate
# 10/64 = ~15% bit difference. Calibrated for article-length text (~200–2000 words).
# Short texts have higher variance — the pipeline skips SimHash for metadata-only articles.
NEAR_DUP_THRESHOLD = 10


def _tokenise(text: str) -> list[str]:
    """Lowercase, strip punctuation, split into words."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = text.split()
    # Remove stop words (short tokens add noise to SimHash)
    stopwords = frozenset(
        [
            "the",
            "a",
            "an",
            "and",
            "or",
            "but",
            "in",
            "on",
            "at",
            "to",
            "for",
            "of",
            "with",
            "by",
            "from",
            "is",
            "was",
            "are",
            "were",
            "be",
            "been",
            "has",
            "have",
            "had",
            "will",
            "would",
            "could",
            "should",
            "its",
            "it",
            "this",
            "that",
            "as",
            "up",
            "down",
        ]
    )
    return [t for t in tokens if len(t) > 2 and t not in stopwords]


def compute_simhash(text: str) -> str:
    """Return SimHash as a zero-padded 16-char hex string."""
    from simhash import Simhash

    tokens = _tokenise(text[:10_000])  # cap for speed
    if not tokens:
        return "0" * 16
    h = Simhash(tokens)
    return format(h.value, "016x")


def simhash_distance(a: str, b: str) -> int:
    """Hamming distance between two SimHash hex strings."""
    ia = int(a, 16)
    ib = int(b, 16)
    xor = ia ^ ib
    return bin(xor).count("1")


def is_near_duplicate(candidate_hash: str, existing_hashes: list[str]) -> bool:
    """True if candidate is within NEAR_DUP_THRESHOLD bits of any existing hash."""
    for h in existing_hashes:
        if simhash_distance(candidate_hash, h) <= NEAR_DUP_THRESHOLD:
            return True
    return False
