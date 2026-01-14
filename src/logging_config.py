import logging
from pathlib import Path

LOG_DIR = Path.cwd() / "data" / "logs"


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "pipeline.log"),
            logging.StreamHandler()
        ]
    )