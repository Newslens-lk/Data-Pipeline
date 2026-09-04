# NewsLens Pipeline - Test Suite

107 tests across 3 layers covering all 6 pipeline containers and shared config.

## Quick Start

```bash
# Activate the virtual environment
source venv/bin/activate

# Run the full suite
python -m pytest tests/ -v

# Run a single layer
python -m pytest tests/unit/ -v
python -m pytest tests/contracts/ -v
python -m pytest tests/integration/ -v

# Run tests for a specific container
python -m pytest tests/unit/test_cleaner.py -v
python -m pytest tests/integration/test_cleaner_integration.py -v
```

## Dependencies

Install test dependencies into the venv:

```bash
pip install boto3 hdbscan psycopg2-binary numpy pydantic pytest requests moto
```

## Directory Structure

```
tests/
├── conftest.py                          # Env vars, load_container(), sample data fixtures
├── README.md
│
├── contracts/                           # Layer 1: NDJSON schema contracts (28 tests)
│   ├── schemas.py                       # Pydantic models for all inter-stage NDJSON
│   └── test_ndjson_contracts.py         # Valid/invalid/cross-stage tests
│
├── unit/                                # Layer 2: Pure function tests (60 tests)
│   ├── test_scraper.py                  # article_id() hashing
│   ├── test_cleaner.py                  # normalize_text, deduplicate, clean_articles
│   ├── test_bias_xgb.py                # Labels, probability-to-label conversion
│   ├── test_bias_transformers.py        # Labels, score conversion, text prep format
│   ├── test_clustering.py              # HDBSCAN batch clustering, KNN majority vote
│   ├── test_config.py                   # Config defaults and immutability
│   └── test_loader.py                   # Join logic, event stats, embedding format
│
└── integration/                         # Layer 3: Container main() flow tests (19 tests)
    ├── conftest.py                      # moto S3 mock, seed/read NDJSON helpers
    ├── test_cleaner_integration.py      # S3 read -> clean -> S3 write
    ├── test_embedder_integration.py     # S3 read -> embed (mock model) -> S3 write
    ├── test_bias_xgb_integration.py     # S3 read -> classify (mock XGB) -> S3 write
    ├── test_bias_transformers_integration.py  # S3 read -> classify (mock HelaBERT) -> S3 write
    ├── test_clustering_integration.py   # S3 read -> KNN/HDBSCAN (mock DB) -> S3 write
    └── test_loader_integration.py       # S3 read all 4 stages -> join -> DB write (mock)
```

## Test Layers

### Layer 1: Contract Tests (28 tests)

Pydantic schemas enforce the NDJSON data contracts between pipeline stages. If a container changes its output format, these tests catch it before it silently breaks the next stage.

**Schemas defined in `contracts/schemas.py`:**

| Schema | Stage | Key validations |
|--------|-------|-----------------|
| `ScraperOutput` | scraper -> cleaner | `article_id` exactly 24 hex chars, `source`/`url` non-empty, `published_at` nullable |
| `CleanerOutput` | cleaner -> embedder/bias | Same shape as scraper, `body` must be non-empty |
| `EmbedderOutput` | embedder -> bias-xgb/clustering/loader | `embedding` list must be non-empty |
| `BiasOutput` | bias-\* -> loader | `bias_label` one of 5 values, `bias_confidence` 0-1, `bias_scores` has all 5 keys |
| `ClusterOutput` | clustering -> loader | `cluster_method` one of knn/hdbscan_new/single, `distance` >= 0 |

**Test groups:**
- `TestValidRecords` - Valid data parses correctly for each schema
- `TestInvalidRecords` - Bad data rejected with clear errors (short IDs, invalid labels, missing keys, etc.)
- `TestCrossStageCompatibility` - Both pipeline paths validated:
  ```
  Path 1 (XGB):          cleaner -> embedder -> bias-xgb ---------> loader
                                        └-----> clustering -------> loader
  Path 2 (Transformers): cleaner -> bias-transformers ------------> loader
                          cleaner -> embedder -> clustering -------> loader
  ```

### Layer 2: Unit Tests (60 tests)

Test individual functions in isolation with no external dependencies (no S3, no DB, no ML models).

| File | Container/Module | What's tested |
|------|-----------------|---------------|
| `test_scraper.py` | scraper | `article_id()` - deterministic SHA256, 24-char hex, unicode URLs |
| `test_cleaner.py` | cleaner | `normalize_text()` - whitespace, zero-width chars, HTML entities, NFC, Sinhala text; `deduplicate()` - exact + near-dup removal; `clean_articles()` - full pipeline |
| `test_bias_xgb.py` | bias-classifier-xgb | 5-class label set, probability-to-label conversion for all bias positions |
| `test_bias_transformers.py` | bias-classifier-transformers | Label set matches XGB, NUM_LABELS constant, text prep format (`title. body`) |
| `test_clustering.py` | clustering | `batch_cluster_unassigned()` - HDBSCAN edge cases (empty, single, groups); `find_nearest_cluster()` - majority vote with mocked DB cursor |
| `test_config.py` | include/config.py | Singleton exists, frozen immutability, defaults are valid |
| `test_loader.py` | loader | Join-by-article_id logic, missing data handling, event stats aggregation, pgvector string format |

### Layer 3: Integration Tests (19 tests)

Test each container's complete `main()` flow: read from S3, process, write to S3/DB. Uses [moto](https://github.com/getmoto/moto) for S3 mocking and `unittest.mock` for DB/ML models.

| File | Container | What's tested |
|------|-----------|---------------|
| `test_cleaner_integration.py` | cleaner | Full clean+write, short article filtering, near-duplicate removal |
| `test_embedder_integration.py` | embedder | Full embed+write (mock model), batch splitting, Modal path |
| `test_bias_xgb_integration.py` | bias-classifier-xgb | Full classify+write (mock XGBoost), confidence validation, score completeness |
| `test_bias_transformers_integration.py` | bias-classifier-transformers | Local classify flow, Modal path, output schema matches XGB |
| `test_clustering_integration.py` | clustering | All-new articles (HDBSCAN/single), KNN match to existing cluster, mixed KNN+single |
| `test_loader_integration.py` | loader | Full 4-file join, source dedup, event aggregation, article rows with all 13 fields |

## How Container Imports Work

Each container has its own `run.py` with top-level env var reads and dependency imports. The test suite handles this with two mechanisms in `conftest.py`:

1. **`container_env` fixture** (session-scoped, autouse) - sets dummy env vars (`STORAGE_ENDPOINT`, `INPUT_KEY`, etc.) before any container module is imported
2. **`load_container(name)` helper** - uses `importlib.util.spec_from_file_location` to load each container's `run.py` as a uniquely-named module (e.g., `container_cleaner_run`), avoiding name collisions between containers that all have `run.py`

Usage in test files:
```python
from tests.conftest import load_container

@pytest.fixture(scope="module")
def cleaner():
    return load_container("cleaner")

def test_something(cleaner):
    result = cleaner.normalize_text("  hello  ")
    assert result == "hello"
```

## Integration Test Mocking Strategy

| External Dependency | Mock Tool | Approach |
|---------------------|-----------|----------|
| S3 / MinIO | `moto.mock_aws` | Full S3 API mock with pre-created bucket |
| PostgreSQL | `unittest.mock.MagicMock` | Mock connection + cursor, capture `execute_values` calls |
| ML models (embedder, HelaBERT) | `unittest.mock.patch` | Replace `load_model()` and batch inference functions |
| Modal remote GPU | `unittest.mock.patch` | Replace `embed_batch_modal()` / `classify_batch_modal()` |

## Adding New Tests

When adding a new pipeline stage or modifying an existing one:

1. **Add/update the pydantic schema** in `contracts/schemas.py`
2. **Add contract tests** in `contracts/test_ndjson_contracts.py` for the new schema and its cross-stage compatibility
3. **Add unit tests** in `unit/test_<container>.py` for any new pure functions
4. **Add integration tests** in `integration/test_<container>_integration.py` testing the full `main()` flow
