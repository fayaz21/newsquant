from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scraper.db.base import Base
import scraper.db.models  # noqa: F401 — register ORM models


@pytest.fixture(scope="function")
def db_engine():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def db_session(db_engine):
    Session = sessionmaker(bind=db_engine, expire_on_commit=False)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def mock_source_rss():
    from scraper.models.source import SourceConfig
    return SourceConfig(
        name="test_rss",
        type="rss",
        enabled=True,
        full_text_required=True,
        rate_limit_rps=999.0,
        feeds=["https://example.com/rss"],
    )


@pytest.fixture
def mock_source_api():
    from scraper.models.source import SourceConfig
    return SourceConfig(
        name="newsapi",
        type="api",
        enabled=True,
        rate_limit_rps=999.0,
        queries=["test query"],
    )


@pytest.fixture
def sample_raw_article():
    from datetime import datetime, timezone
    from scraper.models.article import RawArticle
    return RawArticle(
        url="https://example.com/news/apple-earnings-2024",
        title="Apple Reports Record Q4 Earnings",
        summary="Apple Inc reported record earnings for Q4 2024.",
        published_at=datetime(2024, 10, 31, 18, 0, 0, tzinfo=timezone.utc),
        source_name="test_rss",
        author="Jane Doe",
    )


@pytest.fixture
def long_body():
    return (
        "Apple Inc reported record fourth-quarter earnings on Thursday, "
        "beating analyst expectations with strong iPhone and services revenue. "
        "The company posted earnings per share of $1.46, up 12% year over year, "
        "while revenue rose 6% to $94.9 billion. "
        "CEO Tim Cook said demand for the iPhone 15 lineup was strong globally. "
        "Services revenue hit an all-time high of $24.2 billion. "
        "Investors reacted positively, sending $AAPL stock up 2% in after-hours trading. "
        "Analysts at Goldman Sachs maintained their buy rating with a $220 target price. "
    ) * 10  # ~160 words × 10 = ~1600 words
