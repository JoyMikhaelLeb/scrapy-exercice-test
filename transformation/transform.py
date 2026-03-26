import argparse
import hashlib
import io
import json
import logging
from datetime import datetime

from bs4 import BeautifulSoup
from minio import Minio
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

# ── Logging ────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ── Config ─────────────────────────────────────────────────────────────────────

MONGO_URI              = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB               = os.getenv("MONGO_DB", "wplace")
LANDING_COLLECTION     = os.getenv("MONGO_COLLECTION", "landing_zone")
TRANSFORMED_COLLECTION = os.getenv("MONGO_TRANSFORMED_COLLECTION", "transformed_zone")

LANDING_BUCKET     = os.getenv("MINIO_BUCKET", "wrc-landing")
TRANSFORMED_BUCKET = os.getenv("MINIO_TRANSFORMED_BUCKET", "wrc-transformed")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")


# ── Helpers ────────────────────────────────────────────────────────────────────

def compute_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get_extension(file_path: str) -> str:
    return os.path.splitext(file_path)[1].lower()


def clean_html(raw_bytes: bytes) -> bytes:
    """
    Parse HTML with BeautifulSoup.
    Extract only div.content — the relevant decision text.
    Strip everything else (nav, header, footer, scripts, cookie banners).
    """
    soup    = BeautifulSoup(raw_bytes, "html.parser")
    content = soup.find("div", class_="content")

    if not content:
        # fallback: try col-sm-9 which also contains main content
        content = soup.find("div", class_="col-sm-9")

    if not content:
        logger.warning("Could not find content div, using full body")
        content = soup.find("body")

    # build a minimal clean HTML document
    clean = f"<html><body>{content}</body></html>"
    return clean.encode("utf-8")


# ── Main Transform ─────────────────────────────────────────────────────────────

def transform(start_date: str, end_date: str):
    """
    Main transformation function.
    - Reads records from MongoDB landing_zone between start_date and end_date
    - For each record: cleans HTML or copies PDF/DOC as-is
    - Renames file to identifier.ext
    - Uploads to wrc-transformed MinIO bucket
    - Saves metadata to MongoDB transformed_zone
    """

    # parse dates
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    # ── Connect ────────────────────────────────────────────────────────────────
    mongo       = MongoClient(MONGO_URI)
    landing_col = mongo[MONGO_DB][LANDING_COLLECTION]
    transformed = mongo[MONGO_DB][TRANSFORMED_COLLECTION]
    transformed.create_index("identifier", unique=True)

    minio = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )

    # create transformed bucket if it doesn't exist
    if not minio.bucket_exists(TRANSFORMED_BUCKET):
        minio.make_bucket(TRANSFORMED_BUCKET)
        logger.info(json.dumps({
            "event":  "bucket_created",
            "bucket": TRANSFORMED_BUCKET,
        }))

    # ── Fetch records from landing zone ────────────────────────────────────────
    # generate list of partition_date values between start and end
    # e.g. ["2024-01", "2024-02", "2024-03"]
    partitions = []
    current = start
    while current < end:
        partitions.append(current.strftime("%Y-%m"))
        # move to next month
        month = current.month + 1
        year  = current.year + (1 if month > 12 else 0)
        month = 1 if month > 12 else month
        current = current.replace(year=year, month=month, day=1)

    records = list(landing_col.find({
        "partition_date": {"$in": partitions}
    }))

    logger.info(json.dumps({
        "event":      "transform_started",
        "start_date": start_date,
        "end_date":   end_date,
        "records":    len(records),
    }))

    success = 0
    failed  = 0
    skipped = 0

    for record in records:
        identifier = record["identifier"]
        file_path  = record.get("file_path")

        if not file_path:
            logger.warning(json.dumps({
                "event":      "skipped",
                "identifier": identifier,
                "reason":     "no file_path in record",
            }))
            skipped += 1
            continue

        ext         = get_extension(file_path)
        object_name = f"{record['partition_date']}/{identifier}{ext}"

        try:
            # ── Download from landing zone ─────────────────────────────────────
            response   = minio.get_object(LANDING_BUCKET, file_path)
            raw_bytes  = response.read()
            response.close()

            # ── Transform ──────────────────────────────────────────────────────
            if ext in (".pdf", ".doc", ".docx"):
                # PDF/DOC: copy as-is, no transformation
                final_bytes = raw_bytes
                logger.info(json.dumps({
                    "event":      "file_copied",
                    "identifier": identifier,
                    "type":       ext,
                }))
            else:
                # HTML: clean with BeautifulSoup
                final_bytes = clean_html(raw_bytes)
                logger.info(json.dumps({
                    "event":      "file_cleaned",
                    "identifier": identifier,
                    "type":       ".html",
                }))

            # ── Compute new hash ───────────────────────────────────────────────
            new_hash = compute_hash(final_bytes)

            # ── Idempotency: skip if already transformed with same hash ─────────
            existing = transformed.find_one({"identifier": identifier})
            if existing and existing.get("file_hash") == new_hash:
                logger.info(json.dumps({
                    "event":      "skipped",
                    "identifier": identifier,
                    "reason":     "unchanged_hash",
                }))
                skipped += 1
                continue

            # ── Upload to transformed bucket ───────────────────────────────────
            minio.put_object(
                TRANSFORMED_BUCKET,
                object_name,
                io.BytesIO(final_bytes),
                length=len(final_bytes),
            )

            # ── Save to transformed_zone collection ────────────────────────────
            transformed_doc = {
                "identifier":     identifier,
                "title":          record.get("title"),
                "description":    record.get("description"),
                "ref_no":         record.get("ref_no"),
                "published_date": record.get("published_date"),
                "body":           record.get("body"),
                "doc_url":        record.get("doc_url"),
                "partition_date": record.get("partition_date"),
                "file_path":      object_name,
                "file_hash":      new_hash,
                "transformed_at": datetime.utcnow().isoformat(),
            }

            transformed.update_one(
                {"identifier": identifier},
                {"$set": transformed_doc},
                upsert=True,
            )

            logger.info(json.dumps({
                "event":       "record_transformed",
                "identifier":  identifier,
                "object_name": object_name,
                "file_hash":   new_hash,
            }))
            success += 1

        except Exception as e:
            logger.error(json.dumps({
                "event":      "transform_failed",
                "identifier": identifier,
                "error":      str(e),
            }))
            failed += 1

    # ── Summary ────────────────────────────────────────────────────────────────
    logger.info(json.dumps({
        "event":      "transform_summary",
        "start_date": start_date,
        "end_date":   end_date,
        "total":      len(records),
        "success":    success,
        "skipped":    skipped,
        "failed":     failed,
    }))

    mongo.close()


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", required=True, help="Format: YYYY-MM-DD")
    parser.add_argument("--end-date",   required=True, help="Format: YYYY-MM-DD")
    args = parser.parse_args()

    transform(args.start_date, args.end_date)