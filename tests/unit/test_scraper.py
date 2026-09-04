"""Unit tests for the scraper container's pure functions."""
from __future__ import annotations

import pytest
from tests.conftest import load_container


@pytest.fixture(scope="module")
def scraper():
    return load_container("scraper")


class TestArticleId:
    def test_deterministic(self, scraper):
        url = "https://hirunews.lk/some-article"
        assert scraper.article_id(url) == scraper.article_id(url)

    def test_length_is_24(self, scraper):
        result = scraper.article_id("https://example.com/any-url")
        assert len(result) == 24

    def test_hex_characters_only(self, scraper):
        result = scraper.article_id("https://example.com/test")
        assert all(c in "0123456789abcdef" for c in result)

    def test_different_urls_produce_different_ids(self, scraper):
        id1 = scraper.article_id("https://example.com/article-1")
        id2 = scraper.article_id("https://example.com/article-2")
        assert id1 != id2

    def test_unicode_url(self, scraper):
        result = scraper.article_id("https://example.com/සිංහල-article")
        assert len(result) == 24
