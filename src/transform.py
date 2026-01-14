"""
Transformation layer for the data pipeline.

This module defines functions that take raw students, courses,
and jobs data extracted from the database and transform them
into clean, analysis-ready DataFrames following business standards.
"""

import json
import pandas as pd
import logging


logger = logging.getLogger(__name__)

# --------------
# Helpers
# --------------

def safe_json_load(value):
    """
    Safely parse a JSON-like value.

    - Returns the parsed dictionary if the value is a valid JSON string.
    - Returns the value as-is if it is already a dictionary.
    - Returns None for null, invalid, or unexpected values.

    Invalid or unexpected inputs are logged as warnings.


    :param value: JSON string, dictionnary, null-like value
    """
    
    if pd.isna(value):
        return None

    if isinstance(value, dict):
        return value
    
    if not isinstance(value, str):
        logger.warning("Unexpected type for JSON field: %s", type(value))
        return None

    try:
        return json.loads(value)
    except (TypeError, ValueError):
        logger.warning("Invalid JSON encountered: %s", value)
        return None
    

# ----------
# Cleaning
# ----------

def clean_students(students: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the students dataset.

    Transformations performed:
    - Convert ID and count fields to nullable integers (Int64)
    - Convert time spent to float hours
    - Parse date of birth into datetime
    - Expand the `contact_info` JSON column into separate columns
    - Drop rows missing uuid

    The original DataFrame is not modified.
    
    :param students: Raw students dataset
    :type students: pd.DataFrame
    :return: Cleaned students dataset
    :rtype: pd.DataFrame
    """
    df = students.copy()

    # ---- IDs & numeric fields ----
    int_cols = [
        "job_id",
        "num_course_taken",
        "current_career_path_id",
    ]

    for col in int_cols:
        df[col] = (
            pd.to_numeric(df[col], errors="coerce")
            .astype("Int64")
        )

    df["time_spent_hrs"] = pd.to_numeric(
        df["time_spent_hrs"], errors="coerce"
    ).astype("float64")

    # ---- DOB ----
    df["dob"] = pd.to_datetime(df["dob"], errors="coerce")

    # ---- contact_info JSON ----
    contact_expanded = pd.json_normalize(
        df["contact_info"].map(safe_json_load)
    )

    df = pd.concat(
        [df.drop(columns=["contact_info"], errors="ignore"),
         contact_expanded],
        axis=1
    )

    # ---- Drop rows with missing primary identifier ----
    before = len(df)
    df = df.dropna(subset=["uuid"])
    after = len(df)

    if before != after:
        logger.warning(
            "Dropped %d student rows due to missing uuid",
            before - after
        )

    return df


def clean_courses(courses: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the courses dataset.

    Transformations performed:
    - Convert career_path_id to nullable integer (Int64)
    - Convert hours_to_complete to float
    - Drop rows missing carrer_path_id

    The original DataFrame is not modified.
    
    :param courses: Raw courses dataset
    :type courses: pd.DataFrame
    :return: Cleaned courses dataset
    :rtype: pd.DataFrame
    """
    df = courses.copy()

    df["career_path_id"] = (
        pd.to_numeric(df["career_path_id"], errors="coerce")
        .astype("Int64")
    )

    df["hours_to_complete"] = (
        pd.to_numeric(df["hours_to_complete"], errors="coerce")
        .astype("float64")
    )

    # ---- Drop rows with missing primary identifier ----
    before = len(df)
    df = df.dropna(subset=["career_path_id"])
    after = len(df)

    if before != after:
        logger.warning(
            "Dropped %d course rows due to missing career_path_id",
            before - after
        )

    return df


def clean_jobs(jobs: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and normalize the jobs dataset.

    Transformations performed:
    - Convert job_id to nullable integer (Int64)
    - Convert avg_salary to float

    The original DataFrame is not modified.

    
    :param jobs: Raw jobs dataset
    :type jobs: pd.DataFrame
    :return: Cleaned jobs dataset
    :rtype: pd.DataFrame
    """
    df = jobs.copy()

    df["job_id"] = (
        pd.to_numeric(df["job_id"], errors="coerce")
        .astype("Int64")
    )

    df["avg_salary"] = (
        pd.to_numeric(df["avg_salary"], errors="coerce")
        .astype("float64")
    )

    # ---- Drop rows with missing primary identifier ----
    before = len(df)
    df = df.dropna(subset=["job_id"])
    after = len(df)

    if before != after:
        logger.warning(
            "Dropped %d job rows due to missing job_id",
            before - after
        )

    return df
    

# -------------------------
# Validation
# -------------------------

def validate_foreign_keys(
    students: pd.DataFrame,
    jobs: pd.DataFrame,
    courses: pd.DataFrame,
) -> None:
    """
    Validate foreign key relationships between datasets.

    Checks:
    - students.job_id exists in jobs.job_id
    - students.current_career_path_id exists in courses.career_path_id

    Any invalid references are logged as warnings.
    This function does not raise exceptions or modify data.
    
    :param students: Cleaned students dataset
    :type students: pd.DataFrame
    :param jobs: Cleaned jobs dataset
    :type jobs: pd.DataFrame
    :param courses: cleaned courses dataset
    :type courses: pd.DataFrame
    """
    # job_id FK
    invalid_job_ids = (
        students["job_id"].notna() &
        ~students["job_id"].isin(jobs["job_id"])
    )

    if invalid_job_ids.any():
        logger.warning(
            "Found %d students with invalid job_id",
            invalid_job_ids.sum()
        )

    # career_path FK
    invalid_career_ids = (
        students["current_career_path_id"].notna() &
        ~students["current_career_path_id"].isin(courses["career_path_id"])
    )

    if invalid_career_ids.any():
        logger.warning(
            "Found %d students with invalid career_path_id",
            invalid_career_ids.sum()
        )
