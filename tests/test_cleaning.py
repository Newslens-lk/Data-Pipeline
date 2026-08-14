"""
Unit tests for include/preprocessing/cleaning.py.

Run with: pytest tests/ -v
These import include/ modules directly, no running Airflow instance needed —
that's the payoff of keeping business logic out of dags/.
"""
from include.preprocessing.cleaning import (
    CleanArticle,
    extract_text,
    near_duplicate_ids,
    normalize_whitespace,
)


def test_normalize_whitespace_collapses_and_trims():
    assert normalize_whitespace("  hello   \n\n world  ") == "hello world"


def test_extract_text_pulls_title_and_paragraphs():
    html = """
    <html><body>
        <h1>Breaking news</h1>
        <p>First paragraph.</p>
        <p>Second paragraph.</p>
    </body></html>
    """
    title, body = extract_text(html)
    assert title == "Breaking news"
    assert "First paragraph." in body
    assert "Second paragraph." in body


def _article(article_id: str, body: str) -> CleanArticle:
    return CleanArticle(
        article_id=article_id, source="s", url=f"https://x/{article_id}",
        title="t", body=body, published_at=None, language="en", scraped_at="now",
    )


def test_near_duplicate_ids_flags_repeated_prefix():
    articles = [
        _article("a1", "The president announced a new policy today in Washington regarding trade."),
        _article("a2", "The president announced a new policy today in Washington regarding trade."),
        _article("a3", "A completely different story about a local sports event happening tonight."),
    ]
    dupes = near_duplicate_ids(articles)
    assert dupes == {"a2"}
