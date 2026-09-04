"""Unit tests for the cleaner container's text processing functions."""
from __future__ import annotations

import json

import pytest
from tests.conftest import load_container


@pytest.fixture(scope="module")
def cleaner():
    return load_container("cleaner")


# ── normalize_text ──


class TestNormalizeText:
    def test_collapses_whitespace(self, cleaner):
        assert cleaner.normalize_text("  hello   \n\n world  ") == "hello world"

    def test_removes_zero_width_chars(self, cleaner):
        text = "hello\u200b\u200c\u200dworld"
        assert cleaner.normalize_text(text) == "helloworld"

    def test_replaces_html_entities(self, cleaner):
        assert cleaner.normalize_text("Tom &amp; Jerry") == "Tom & Jerry"
        assert cleaner.normalize_text("a &lt; b &gt; c") == "a < b > c"
        assert cleaner.normalize_text("&quot;quoted&quot;") == '"quoted"'
        assert cleaner.normalize_text("it&#39;s") == "it's"

    def test_nfc_normalization(self, cleaner):
        import unicodedata
        decomposed = unicodedata.normalize("NFD", "café")
        result = cleaner.normalize_text(decomposed)
        assert result == unicodedata.normalize("NFC", "café")

    def test_sinhala_text_preserved(self, cleaner):
        sinhala = "මෙය පරීක්ෂණ පෙළකි"
        result = cleaner.normalize_text(sinhala)
        assert result == sinhala

    def test_mixed_sinhala_english(self, cleaner):
        text = "  Breaking: මෙය   පුවතකි  today  "
        result = cleaner.normalize_text(text)
        assert result == "Breaking: මෙය පුවතකි today"

    def test_empty_string(self, cleaner):
        assert cleaner.normalize_text("") == ""

    def test_only_invisible_chars(self, cleaner):
        text = "\u200b\u200c\u200d\ufeff"
        assert cleaner.normalize_text(text) == ""

    def test_nbsp_entity(self, cleaner):
        assert cleaner.normalize_text("word&nbsp;word") == "word word"

    def test_bom_removed(self, cleaner):
        text = "\ufeffHello"
        assert cleaner.normalize_text(text) == "Hello"


# ── deduplicate ──


class TestDeduplicate:
    def test_removes_exact_duplicate_ids(self, cleaner):
        articles = [
            {"article_id": "aaa", "body": "First article body text here."},
            {"article_id": "aaa", "body": "Different body same id."},
            {"article_id": "bbb", "body": "Second article unique."},
        ]
        result = cleaner.deduplicate(articles)
        assert len(result) == 2
        assert result[0]["article_id"] == "aaa"
        assert result[1]["article_id"] == "bbb"

    def test_removes_near_duplicates_by_prefix(self, cleaner):
        shared_prefix = "x" * 300
        articles = [
            {"article_id": "a1", "body": shared_prefix + " unique ending one"},
            {"article_id": "a2", "body": shared_prefix + " unique ending two"},
            {"article_id": "a3", "body": "Completely different article content here."},
        ]
        result = cleaner.deduplicate(articles)
        assert len(result) == 2
        ids = {a["article_id"] for a in result}
        assert "a1" in ids
        assert "a3" in ids

    def test_keeps_articles_with_different_prefixes(self, cleaner):
        articles = [
            {"article_id": "a1", "body": "First story about economics and trade."},
            {"article_id": "a2", "body": "Second story about sports and cricket."},
        ]
        result = cleaner.deduplicate(articles)
        assert len(result) == 2

    def test_empty_list(self, cleaner):
        assert cleaner.deduplicate([]) == []

    def test_single_article(self, cleaner):
        articles = [{"article_id": "a1", "body": "Only article."}]
        result = cleaner.deduplicate(articles)
        assert len(result) == 1


# ── clean_articles ──


class TestCleanArticles:
    def test_normalizes_and_filters(self, cleaner):
        raw_ndjson = "\n".join([
            json.dumps({
                "article_id": "a1",
                "title": "  Hello  &amp;  World  ",
                "body": "A" * 100,
            }),
            json.dumps({
                "article_id": "a2",
                "title": "Short",
                "body": "tiny",  # too short, should be filtered
            }),
        ])
        result = cleaner.clean_articles(raw_ndjson)
        assert len(result) == 1
        assert result[0]["title"] == "Hello & World"

    def test_deduplicates(self, cleaner):
        article = {
            "article_id": "a1",
            "title": "Title",
            "body": "B" * 100,
        }
        raw_ndjson = "\n".join([
            json.dumps(article),
            json.dumps({**article, "article_id": "a2"}),  # same body -> near-dup
        ])
        result = cleaner.clean_articles(raw_ndjson)
        assert len(result) == 1

    def test_empty_input(self, cleaner):
        assert cleaner.clean_articles("") == []
