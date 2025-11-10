import logging
from datetime import datetime
from pathlib import Path

def setup_logging():
    Path("logs").mkdir(exist_ok=True)
    log_filename = f"logs/ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging to file: {log_filename}")
