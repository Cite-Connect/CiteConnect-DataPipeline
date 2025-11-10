import logging
from google.cloud import storage


def upload_to_gcs(local_path, bucket_name, destination_path):
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
