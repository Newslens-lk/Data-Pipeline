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
