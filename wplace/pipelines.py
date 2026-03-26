import hashlib
import io
import logging
import requests
import json

from minio import Minio
from pymongo import MongoClient

logger = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def compute_hash(data: bytes) -> str:
    """Calculate SHA-256 hash of file bytes."""
    return hashlib.sha256(data).hexdigest()


def get_file_extension(url: str) -> str:
    """Guess file extension from URL."""
    url_lower = url.lower().split("?")[0]
    if url_lower.endswith(".pdf"):
        return ".pdf"
    elif url_lower.endswith(".doc"):
        return ".doc"
    elif url_lower.endswith(".docx"):
        return ".docx"
    else:
        return ".html"


# ── MongoDB Pipeline ───────────────────────────────────────────────────────────

class MongoPipeline:

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.mongo_uri        = mongo_uri
        self.mongo_db         = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        """Scrapy calls this to create the pipeline with settings."""
        return cls(
            mongo_uri        = crawler.settings.get("MONGO_URI"),
            mongo_db         = crawler.settings.get("MONGO_DB"),
            mongo_collection = crawler.settings.get("MONGO_COLLECTION"),
        )

    def open_spider(self):
        self.client = MongoClient(self.mongo_uri)
        self.col    = self.client[self.mongo_db][self.mongo_collection]
        self.col.create_index("identifier", unique=True)
        logger.info("MongoPipeline connected")

    def close_spider(self):
        self.client.close()
        logger.info("MongoPipeline disconnected")

    def process_item(self, item, spider=None):
        doc = dict(item)
        self.col.update_one(
            {"identifier": item["identifier"]},
            {"$setOnInsert": doc},
            upsert=True,
        )
        logger.info(json.dumps({
            "event":          "item_saved",
            "identifier":     item["identifier"],
            "partition_date": item["partition_date"],
            "body":           item["body"],
        }))
        return item


# ── MinIO Pipeline ─────────────────────────────────────────────────────────────

class MinioPipeline:

    def __init__(self, endpoint, access_key, secret_key, bucket, mongo_uri, mongo_db, mongo_collection):
        self.endpoint         = endpoint
        self.access_key       = access_key
        self.secret_key       = secret_key
        self.bucket           = bucket
        self.mongo_uri        = mongo_uri
        self.mongo_db         = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            endpoint         = crawler.settings.get("MINIO_ENDPOINT"),
            access_key       = crawler.settings.get("MINIO_ACCESS_KEY"),
            secret_key       = crawler.settings.get("MINIO_SECRET_KEY"),
            bucket           = crawler.settings.get("MINIO_BUCKET"),
            mongo_uri        = crawler.settings.get("MONGO_URI"),
            mongo_db         = crawler.settings.get("MONGO_DB"),
            mongo_collection = crawler.settings.get("MONGO_COLLECTION"),
        )

    def open_spider(self):
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )
        self.mongo = MongoClient(self.mongo_uri)
        self.col   = self.mongo[self.mongo_db][self.mongo_collection]

        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)
            logger.info(f"Created MinIO bucket: {self.bucket}")

    def close_spider(self):
        self.mongo.close()
        logger.info("MinioPipeline disconnected")

    def process_item(self, item, spider=None):
        url = item.get("doc_url")
        if not url:
            return item

        ext         = get_file_extension(url)
        object_name = f"{item['partition_date']}/{item['identifier']}{ext}"

        try:
            response   = requests.get(url, timeout=30)
            response.raise_for_status()
            file_bytes = response.content
            file_hash  = compute_hash(file_bytes)

            existing = self.col.find_one({"identifier": item["identifier"]})
            if existing and existing.get("file_hash") == file_hash:
                logger.info(json.dumps({
                    "event":      "file_skipped",
                    "identifier": item["identifier"],
                    "reason":     "unchanged_hash",
                }))
                return item

            self.client.put_object(
                self.bucket,
                object_name,
                io.BytesIO(file_bytes),
                length=len(file_bytes),
            )

            self.col.update_one(
                {"identifier": item["identifier"]},
                {"$set": {
                    "file_path": object_name,
                    "file_hash": file_hash,
                }}
            )
            logger.info(json.dumps({
                    "event":       "file_uploaded",
                    "identifier":  item["identifier"],
                    "object_name": object_name,
                }))

        except Exception as e:
            logger.error(json.dumps({
                "event":      "file_failed",
                "identifier": item["identifier"],
                "url":        url,
                "error":      str(e),
            }))

        return item