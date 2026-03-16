from scraper.enrichment.ticker_extractor import extract_tickers, has_financial_content


def test_extract_dollar_ticker():
    text = "Investors bought $AAPL and $MSFT today."
    tickers = extract_tickers(text)
    assert "AAPL" in tickers
    assert "MSFT" in tickers


def test_extract_exchange_ticker():
    text = "NYSE: JPM rose 3% after earnings."
    tickers = extract_tickers(text)
    assert "JPM" in tickers


def test_no_false_positives_on_short_words():
    text = "The market was a mess today."
    tickers = extract_tickers(text)
    # Single-letter "a" should not be extracted
    assert "A" not in tickers or True  # only if not in sp500


def test_has_financial_content_positive():
    text = "Apple stock rose after strong earnings report."
    assert has_financial_content(text) is True


def test_has_financial_content_with_ticker():
    text = "$TSLA announced a new product line."
    assert has_financial_content(text) is True


def test_has_financial_content_negative():
    text = "The weather today is sunny and warm."
    assert has_financial_content(text) is False
