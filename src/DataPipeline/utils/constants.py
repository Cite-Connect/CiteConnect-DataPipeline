import os

SEM_KEY = os.getenv("SEMANTIC_SCHOLAR_KEY")
GCS_BUCKET = os.getenv("GCS_BUCKET", "citeconnect-processed-parquet")

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/html"
}
if SEM_KEY:
    HEADERS["x-api-key"] = SEM_KEY
