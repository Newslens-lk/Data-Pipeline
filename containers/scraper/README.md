# Scraper Container

Scrapes Sinhala news articles using the [lk_news](https://github.com/nuuuwan/lk_news) package and writes them as NDJSON to object storage.

## Sources

| Class | Site | Status |
|-------|------|--------|
| DivainaLk | divaina.lk | Working |
| LankadeepaLk | lankadeepa.lk | 0 results (site structure may have changed) |
| AdaDeranaSinhalaLk | sinhala.adaderana.lk | Skipped (selenium `read_selenium` broken in upstream package) |
| AdaLk | ada.lk | Removed (504 timeouts) |

## Output contract

One NDJSON line per article:
```json
{"article_id": "sha256(url)[:24]", "source": "divaina-lk", "url": "...", "title": "...", "body": "...", "language": "si", "published_at": "2026-08-15T10:00:00", "scraped_at": "2026-08-15T12:00:00"}
```

Written to: `s3://{STORAGE_BUCKET}/raw/{RUN_DATE}/articles.ndjson`

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `STORAGE_ENDPOINT` | Yes | S3/MinIO endpoint (e.g. `http://minio:9000`) |
| `STORAGE_BUCKET` | Yes | Bucket name (e.g. `newslens-pipeline`) |
| `AWS_ACCESS_KEY_ID` | Yes | S3/MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | Yes | S3/MinIO secret key |
| `RUN_DATE` | No | Date prefix for output key (default: today) |
| `SCRAPE_TIMEOUT` | No | Max seconds per source (default: 300) |

## Build and run

```bash
docker build -t newslens/scraper:v1 .
docker run --rm --network newslens_pipeline_default --env-file ../../.env newslens/scraper:v1
```

## Adding a new source

1. Create a class extending `AbstractNewsPaper` (from `news_lk3.core`):
   ```python
   from news_lk3.core import AbstractNewsPaper

   class HiruNewsLk(AbstractNewsPaper):
       @classmethod
       def get_original_lang(cls):
           return "si"

       @classmethod
       def get_index_urls(cls):
           return ["https://www.hirunews.lk/sinhala/"]

       @classmethod
       def parse_article_urls(cls, soup):
           # extract article links from index page
           ...

       @classmethod
       def parse_title(cls, soup):
           ...

       @classmethod
       def parse_body_lines(cls, soup):
           ...
   ```

2. Import it in `run.py` and add to `SINHALA_SOURCES` and `source_classes`
3. Rebuild: `docker build -t newslens/scraper:v2 .`
4. Update Airflow Variable: `airflow variables set scraper_image newslens/scraper:v2`

## How web scraping works

Web scraping is reading a website's HTML and extracting the data you need from it. A browser renders HTML into the visual page you see — scraping skips the visual part and works directly with the raw HTML structure.

### The two-step process

Every scraper follows two steps:

1. **Find article URLs** — Visit an index page (e.g. a homepage or category page) and extract all the links that point to individual articles.
2. **Parse each article** — Visit each article URL and extract the title, body text, date, etc. from the HTML.

### How lk_news does it (AbstractNewsPaper pattern)

The `lk_news` package uses a base class called `AbstractNewsPaper`. Each news source is a subclass that implements these methods:

| Method | What it does |
|--------|-------------|
| `get_original_lang()` | Returns the language code (e.g. `"si"` for Sinhala) |
| `get_index_urls()` | Returns a list of index page URLs to find articles on |
| `parse_article_urls(soup)` | Given the parsed HTML of an index page, returns a list of article URLs |
| `parse_title(soup)` | Given the parsed HTML of an article page, extracts the title |
| `parse_body_lines(soup)` | Given the parsed HTML of an article page, extracts the body as a list of lines |

The `soup` parameter is a BeautifulSoup object — a Python library that turns raw HTML into a tree you can query (find tags by name, class, id, etc.).

The base class handles the common logic: fetch index pages → call `parse_article_urls` → fetch each article → call `parse_title` and `parse_body_lines` → package into an article object. You only write the parts that differ per site.

### What this container does with the articles

1. Calls `cls.gen_articles()` for each source class (DivainaLk, LankadeepaLk)
2. Skips articles with very short bodies (< 50 chars)
3. Converts each article to our NDJSON contract format (article_id, source, url, title, body, etc.)
4. Uploads the NDJSON to MinIO/S3
5. Prints the storage key to stdout so Airflow can capture it via XCom

### Why sources break

Each source's scraper is tightly coupled to that site's HTML structure. When a site redesigns or changes its HTML layout, the CSS selectors / tag lookups in `parse_article_urls`, `parse_title`, or `parse_body_lines` stop matching, and the scraper returns 0 results or errors. This is normal — you just need to inspect the new HTML and update the parsing logic.

Some sites also use JavaScript to render content (e.g. AdaDerana). These need a headless browser (Selenium/Playwright) instead of simple HTTP requests, which is slower and more fragile.

### RSS as an alternative

Some news sites provide RSS feeds — structured XML with article titles, URLs, and sometimes summaries. RSS is more stable than scraping HTML because it's a deliberate API, not a side effect of page structure. If a source offers RSS, prefer it over HTML scraping.
