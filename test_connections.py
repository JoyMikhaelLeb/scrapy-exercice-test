from pymongo import MongoClient
from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()

# ── Test MongoDB ───────────────────────────────────────────────────────────────
print("Testing MongoDB...")
client = MongoClient(os.getenv("MONGO_URI"))
db = client[os.getenv("MONGO_DB")]
db["test"].insert_one({"test": "hello"})
print("✅ MongoDB connected and inserted a test document")
client.close()

# ── Test MinIO ─────────────────────────────────────────────────────────────────
print("\nTesting MinIO...")
minio = Minio(
    os.getenv("MINIO_ENDPOINT"),
    access_key=os.getenv("MINIO_ACCESS_KEY"),
    secret_key=os.getenv("MINIO_SECRET_KEY"),
    secure=False,
)
bucket = os.getenv("MINIO_BUCKET")
if not minio.bucket_exists(bucket):
    minio.make_bucket(bucket)
    print(f"✅ MinIO connected and created bucket: {bucket}")
else:
    print(f"✅ MinIO connected, bucket already exists: {bucket}")