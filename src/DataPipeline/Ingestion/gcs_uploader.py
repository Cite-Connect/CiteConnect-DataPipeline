import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

# Thread pool for blocking GCS operations
_gcs_executor = ThreadPoolExecutor(max_workers=10, thread_name_prefix="gcs_uploader")


def upload_to_gcs(local_path, bucket_name, destination_path):
    """Synchronous GCS upload"""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_path)
        blob.upload_from_filename(local_path)

        logging.info(f"Uploaded to GCS: gs://{bucket_name}/{destination_path}")
        return True

    except Exception as e:
        logging.error(f"GCS upload failed: {e}")
        return False


async def upload_to_gcs_async(local_path: str, bucket_name: str, destination_path: str) -> bool:
    """
    Async wrapper for GCS upload (runs blocking operation in thread pool)
    
    Args:
        local_path: Local file path
        bucket_name: GCS bucket name
        destination_path: Destination path in bucket
    
    Returns:
        True if successful, False otherwise
    """
    loop = asyncio.get_event_loop()
    
    try:
        result = await loop.run_in_executor(
            _gcs_executor,
            upload_to_gcs,
            local_path,
            bucket_name,
            destination_path
        )
        return result
    except Exception as e:
        logging.error(f"Async GCS upload failed: {e}")
        return False
