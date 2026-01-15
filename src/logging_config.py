import logging
from pathlib import Path

LOG_DIR = Path.cwd() / "data" / "logs"


def setup_logging(level=logging.INFO):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "pipeline.log"),
            logging.StreamHandler()
        ]
    )