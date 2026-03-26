from dotenv import load_dotenv
import os

load_dotenv()

# ── Project ────────────────────────────────────────────
BOT_NAME = "wplace"
SPIDER_MODULES = ["wplace.spiders"]
NEWSPIDER_MODULE = "wplace.spiders"

# ── Anti-bot ───────────────────────────────────────────
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 1
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 4
CONCURRENT_REQUESTS_PER_DOMAIN = 2

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en",
}


# ── Extensions ─────────────────────────────────────────────────────────────────
EXTENSIONS = {
    "wplace.extensions.JsonLoggingExtension": 500,
}

# ── Middlewares ────────────────────────────────────────
DOWNLOADER_MIDDLEWARES = {
    "wplace.middlewares.WplaceMiddleware": 543,
}

# ── Pipelines ──────────────────────────────────────────
ITEM_PIPELINES = {
    "wplace.pipelines.MongoPipeline": 300,
    "wplace.pipelines.MinioPipeline": 400,
}

# ── MongoDB ────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB", "wplace")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "landing_zone")

# ── MinIO ──────────────────────────────────────────────
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET", "wrc-landing")

# ── Logging ────────────────────────────────────────────
LOG_LEVEL = "INFO"