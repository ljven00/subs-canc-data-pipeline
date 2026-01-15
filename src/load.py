"""
Load cleaned data into the analytics database.
"""

import logging
import sqlite3
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


def load_to_sqlite(
    students: pd.DataFrame,
    courses: pd.DataFrame,
    jobs: pd.DataFrame,
    db_path: Path,
) -> None:
    """
    Load cleaned DataFrames into a SQLite database.

    Existing tables are replaced to ensure idempotency.

    :param students: Cleaned students dataset
    :type students: pd.DataFrame
    :param courses: Cleaned courses dataset
    :type courses: pd.DataFrame
    :param jobs: Cleaned jobs dataset
    :type pd.DataFrame
    :param db_path: Path to target SQLite database
    :type db_path: Path
    """
    db_path = Path(db_path)

    logger.info("Loading data into database: %s", db_path)

    try:
        with sqlite3.connect(db_path) as conn:
            students.to_sql(
                "students",
                conn,
                if_exists="replace",
                index=False
            )
            courses.to_sql(
                "courses",
                conn,
                if_exists="replace",
                index=False
            )
            jobs.to_sql(
                "jobs",
                conn,
                if_exists="replace",
                index=False
            )

        logger.info("Data successfully loaded")

    except Exception:
        logger.exception("Failed to load data")
        raise
