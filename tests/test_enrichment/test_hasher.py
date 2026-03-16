from scraper.enrichment.hasher import content_hash, normalize_url, url_hash


def test_normalize_url_strips_tracking():
    url = "https://example.com/article?utm_source=twitter&utm_medium=social&id=123"
    normalized = normalize_url(url)
    assert "utm_source" not in normalized
    assert "id=123" in normalized


def test_normalize_url_strips_fragment():
    url = "https://example.com/article#section1"
    normalized = normalize_url(url)
    assert "#" not in normalized


def test_url_hash_stable():
    url = "https://example.com/news/apple-earnings"
    h1 = url_hash(url)
    h2 = url_hash(url)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex


def test_url_hash_ignores_tracking():
    url1 = "https://example.com/news/apple?utm_source=email"
    url2 = "https://example.com/news/apple"
    assert url_hash(url1) == url_hash(url2)


def test_content_hash_normalizes_whitespace():
    text1 = "Apple reports  earnings   today"
    text2 = "Apple reports earnings today"
    assert content_hash(text1) == content_hash(text2)


def test_content_hash_case_insensitive():
    assert content_hash("Apple Reports Earnings") == content_hash("apple reports earnings")
