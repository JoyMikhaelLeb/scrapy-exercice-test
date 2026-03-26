# Architecture Write-up

## Date Partition Size

Monthly partitions were chosen as the default partition size. The WRC
website returns paginated results filtered by date range. A monthly
window produces a manageable number of pages per request (typically
5-25 pages per body per month), avoiding both overly large responses
and excessive HTTP overhead from daily partitions. For bodies with
lower document volume (e.g. Equality Tribunal), monthly partitions
are even more efficient. The partition size is not hardcoded — it can
be adjusted by modifying `_generate_monthly_partitions()` in the spider
to support weekly or daily partitions if volume increases significantly.

---

## Retries and Rate Limiting

Scrapy's built-in `RetryMiddleware` handles transient failures
automatically, retrying failed requests up to 2 times by default.
Rate limiting is handled at two levels:

- **`DOWNLOAD_DELAY = 1`** introduces a 1-second pause between requests
- **`RANDOMIZE_DOWNLOAD_DELAY = True`** randomizes the delay between
  0.5s and 1.5s, making the scraper behave more like a human browser
- **`CONCURRENT_REQUESTS = 4`** and **`CONCURRENT_REQUESTS_PER_DOMAIN = 2`**
  limit parallel requests to avoid overwhelming the server
- **User-agent rotation** in `WplaceMiddleware` cycles through 5 real
  browser user-agent strings per request to reduce bot detection risk

File downloads in `MinioPipeline` use a 30-second timeout via
`requests.get(url, timeout=30)` with a try/except that logs any
failed download with its URL and error message without crashing
the pipeline.

---

## Deduplication Strategy

Deduplication operates at two levels:

**Record level** — MongoDB uses a unique index on `identifier`.
All inserts use `update_one` with `upsert=True` and `$setOnInsert`,
meaning if a record with the same identifier already exists it is
left untouched. Running the pipeline twice on the same date range
produces exactly the same 278 records — no duplicates.

**File level** — Every file is hashed with SHA-256 before upload.
Before re-uploading, the pipeline compares the new hash against the
stored hash in MongoDB. If they match, the file is skipped entirely.
This means unchanged decisions are never re-downloaded or re-uploaded,
saving both bandwidth and storage. If a decision is updated on the
WRC website, its hash will differ and the pipeline will automatically
re-download and update it.

---

## Scaling to 50+ Sources

The current architecture is intentionally source-specific — CSS
selectors, URL patterns, and body IDs are hardcoded for the WRC
website. To support 50+ sources, the following changes would be made:

**Spider abstraction** — Extract a `BaseSpider` class with shared
logic (pagination, partitioning, error handling, logging) and create
one subclass per source with only the source-specific selectors and
URL patterns. Each source spider would be registered in a central
config file.

**Configuration-driven scraping** — Move selectors, base URLs, body
mappings, and partition sizes into a per-source YAML or JSON config
file. The spider reads its config at runtime rather than having it
hardcoded.

**Distributed execution** — Replace `subprocess.run` in Dagster ops
with proper Dagster assets and partitioned runs. Use Dagster's
built-in partitioning system to run each source and each month as
an independent partition, enabling parallel execution across multiple
workers.

**Separate storage namespaces** — Use one MongoDB collection and one
MinIO bucket prefix per source (e.g. `wrc/landing_zone`,
`ireland_courts/landing_zone`) to keep data isolated and queryable
per source.

**Centralized monitoring** — Emit all structured JSON logs to a
central log aggregator (e.g. Elasticsearch or CloudWatch) and set
up alerts on `records_failed > 0` or `finish_reason != finished`
across all sources.