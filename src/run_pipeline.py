"""
End-to-end pipeline runner.
"""

from pathlib import Path
import logging

from src.extract import extract_all
from src.transform import (
    clean_students,
    clean_courses,
    clean_jobs,
    validate_foreign_keys,
)
from src.load import load_to_sqlite
from src.logging_config import setup_logging


def main():
    setup_logging()

    RAW_DB = Path("data/raw/cademycode.db")

    CLEAN_DB_PATH = Path("data/processed")
    CLEAN_DB_PATH.mkdir(parents=True, exist_ok=True)
    
    clean_db = CLEAN_DB_PATH / "analytics.db"

    raw_data = extract_all(RAW_DB)

    students = clean_students(raw_data["students"])
    courses = clean_courses(raw_data["courses"])
    jobs = clean_jobs(raw_data["jobs"])

    validate_foreign_keys(students, jobs, courses)

    load_to_sqlite(students, courses, jobs, clean_db)


if __name__ == "__main__":
    main()
