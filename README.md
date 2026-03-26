# Kedra WRC Scraping Pipeline

A scalable scraping pipeline that collects legal decisions from the
Workplace Relations Commission (WRC) website, stores them in MongoDB
and MinIO, and transforms the raw HTML into clean content.

---

## Tech Stack

- **Scrapy** — web scraping framework
- **MongoDB** — NoSQL database for metadata storage
- **MinIO** — object storage for document files
- **Docker** — runs MongoDB and MinIO in containers
- **Dagster** — pipeline orchestration
- **BeautifulSoup** — HTML parsing and cleaning

---

## Project Structure
```
kedra-scrapy/
├── wplace/                    # Scrapy project
│   ├── spiders/
│   │   └── wplace_spider.py   # main spider
│   ├── items.py               # data structure
│   ├── pipelines.py           # MongoDB + MinIO storage
│   ├── middlewares.py         # anti-bot (user-agent rotation)
│   ├── extensions.py          # JSON structured logging
│   └── settings.py            # Scrapy configuration
├── transformation/
│   └── transform.py           # HTML cleaning + renaming script
├── dagster_pipeline/
│   └── pipeline.py            # Dagster orchestration
├── docker-compose.yml         # MongoDB + MinIO containers
├── .env.example               # environment variables template
├── requirements.txt           # Python dependencies
├── README.md
└── ARCHITECTURE.md
```

---

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/JoyMikhaelLeb/scrapy-exercice-test/
cd kedra-scrapy
```

### 2. Create and activate virtual environment
```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure environment variables
```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials:
- Set `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` to your chosen values
- Make sure `docker-compose.yml` matches the same credentials

### 5. Start MongoDB and MinIO
```bash
docker compose up -d
```

Verify containers are running:
```bash
docker ps
```

MinIO UI is available at `http://localhost:9001`

---

## Running the Pipeline

### Option A — Dagster UI (recommended)

Launch the Dagster web interface:
```bash
dagster dev -f dagster_pipeline/pipeline.py
```

Open `http://localhost:3000`, go to **Jobs → wplace_pipeline → Launchpad**
and paste this config:
```yaml
ops:
  scrape_op:
    config:
      start_date: "01-01-2024"
      end_date: "01-02-2024"
      body: "Labour Court"
  transform_op:
    config:
      start_date: "2024-01-01"
      end_date: "2024-02-01"
```

Click **Launch Run**.

### Option B — Command line

**Step 1: Run the scraper**
```bash
python -m scrapy crawl wplace \
  -a start_date=01-01-2024 \
  -a end_date=01-02-2024
```

Scrape a single body only (for testing):
```bash
python -m scrapy crawl wplace \
  -a start_date=01-01-2024 \
  -a end_date=01-02-2024 \
  -a body="Labour Court"
```

**Step 2: Run the transformation**
```bash
python transformation/transform.py \
  --start-date 2024-01-01 \
  --end-date 2024-02-01
```

---

## Date Format

| Tool | Format | Example |
|------|--------|---------|
| Spider (`-a`) | DD-MM-YYYY | `01-01-2024` |
| Transform (`--start-date`) | YYYY-MM-DD | `2024-01-01` |

---

## Bodies Available

| Body | Description |
|------|-------------|
| Employment Appeals Tribunal | Pre-2015 employment appeals |
| Equality Tribunal | Equality and discrimination cases |
| Labour Court | Labour relations disputes |
| Workplace Relations Commission | Post-2015 adjudication decisions |

---

## Idempotency

Running the pipeline twice on the same date range will not create
duplicate records or re-download unchanged files. The pipeline uses:
- MongoDB `upsert` with `$setOnInsert` to prevent duplicate records
- SHA-256 file hash comparison to skip unchanged files

---

## Data Flow
```
WRC Website
    ↓
Scrapy Spider (wplace_spider.py)
    ↓
MongoPipeline → MongoDB (landing_zone)
MinioPipeline → MinIO (wrc-landing bucket)
    ↓
transform.py
    ↓
MongoDB (transformed_zone) + MinIO (wrc-transformed bucket)
```

---

## Stopping the containers
```bash
docker compose down
```

To also delete all stored data:
```bash
docker compose down -v
```
