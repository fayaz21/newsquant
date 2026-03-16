from scraper.enrichment.near_duplicate import (
    NEAR_DUP_THRESHOLD,
    compute_simhash,
    is_near_duplicate,
    simhash_distance,
)
from scraper.enrichment.domain_filter import (
    is_paywalled_domain,
    is_scraper_blocked,
    title_similarity,
)


def test_identical_text_distance_zero():
    text = "Apple reported record earnings beating analyst expectations."
    h1 = compute_simhash(text)
    h2 = compute_simhash(text)
    assert simhash_distance(h1, h2) == 0


def test_very_different_text_high_distance():
    h1 = compute_simhash("Apple stock rose after earnings beat expectations quarterly profit")
    h2 = compute_simhash("hurricane season forecast tropical storm caribbean ocean weather")
    dist = simhash_distance(h1, h2)
    assert dist > NEAR_DUP_THRESHOLD


def test_near_duplicate_detected():
    # Use article-length text for reliable SimHash distance measurement
    base = (
        "Apple Inc reported record fourth quarter earnings beating analyst expectations "
        "revenue rose six percent to ninety four billion chief executive tim cook said "
        "demand iphone fifteen lineup strong globally services revenue hit alltime high "
        "investors reacted positively stock rose afterhours trading goldman sachs buy rating "
        "earnings per share dollar forty six up twelve percent year over year strong results "
    ) * 3
    variant = base.replace("rose", "increased").replace("strong", "robust")
    h1 = compute_simhash(base)
    h2 = compute_simhash(variant)
    assert is_near_duplicate(h2, [h1]) is True


def test_different_story_not_flagged():
    story_a = (
        "Apple stock rose after earnings beat expectations quarterly profit revenue "
        "iphone sales services strong analyst upgrade goldman sachs buy rating target "
    ) * 5
    story_b = (
        "Federal Reserve holds interest rates steady inflation concerns economy growth "
        "powell testimony congress monetary policy unemployment jobs report labor market "
    ) * 5
    h1 = compute_simhash(story_a)
    h2 = compute_simhash(story_b)
    assert is_near_duplicate(h2, [h1]) is False


def test_paywall_domain_detection():
    assert is_paywalled_domain("https://www.wsj.com/articles/some-story") is True
    assert is_paywalled_domain("https://www.bloomberg.com/news/article") is True
    assert is_paywalled_domain("https://www.cnbc.com/article") is False
    assert is_paywalled_domain("https://finance.yahoo.com/news/story") is False


def test_scraper_blocked_domain():
    assert is_scraper_blocked("https://www.investing.com/news/story") is True
    assert is_scraper_blocked("https://www.cnbc.com/article") is False


def test_title_similarity_identical():
    assert title_similarity("Apple beats earnings", "Apple beats earnings") == 1.0


def test_title_similarity_partial():
    score = title_similarity("Apple beats Q4 earnings", "Apple Q4 earnings beat expectations")
    assert 0.3 < score < 1.0


def test_title_similarity_unrelated():
    score = title_similarity("Apple earnings beat", "Hurricane season forecast")
    assert score == 0.0
