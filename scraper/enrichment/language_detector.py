from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def detect_language(text: str, confidence_threshold: float = 0.95) -> tuple[str, float]:
    """Returns (language_code, confidence). Falls back to ('unknown', 0.0) on error."""
    try:
        from langdetect import detect_langs

        results = detect_langs(text[:2000])  # cap text for speed
        if results:
            top = results[0]
            return top.lang, top.prob
    except Exception as exc:
        logger.debug("langdetect failed: %s", exc)
    return "unknown", 0.0


def is_english(text: str, confidence_threshold: float = 0.95) -> tuple[bool, str, float]:
    """Returns (is_english, lang, confidence)."""
    lang, conf = detect_language(text, confidence_threshold)
    return (lang == "en" and conf >= confidence_threshold), lang, conf
