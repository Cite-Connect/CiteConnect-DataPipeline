import os
import logging
from google.cloud import storage
from pathlib import Path

def upload_to_gcs(local_path: str, bucket_name: str, destination_blob: str, create_dirs: bool = True):
    """
    Uploads a file to Google Cloud Storage bucket.

    Args:
        local_path (str): Local file path to upload.
        bucket_name (str): GCS bucket name (e.g. 'citeconnect-raw-pdfs').
        destination_blob (str): Path in the bucket (e.g. 'healthcare/AI_in_healthcare.parquet').
        create_dirs (bool): If True, creates intermediate directories in the blob path (default: True).
    
    Returns:
        bool: True if upload successful, False otherwise.
    """
    try:
        if not os.path.exists(local_path):
            logging.error(f"❌ Local file not found: {local_path}")
            return False

        # Ensure we have credentials
        if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
            logging.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS not set. Upload may fail.")

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)

        # Upload file with metadata
        file_size = os.path.getsize(local_path)
        logging.info(f"📤 Uploading {os.path.basename(local_path)} ({file_size} bytes) to gs://{bucket_name}/{destination_blob}")
        
        blob.upload_from_filename(local_path)
        
        logging.info(f"✅ Successfully uploaded to GCS: gs://{bucket_name}/{destination_blob}")
        return True

    except FileNotFoundError:
        logging.error(f"❌ Local file not found: {local_path}")
        return False
    except Exception as e:
        logging.error(f"⚠️ Failed to upload {local_path} to GCS: {e}")
        import traceback
        logging.debug(traceback.format_exc())
        return False
