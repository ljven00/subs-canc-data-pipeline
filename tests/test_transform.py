# tests/test_transform.py

import pandas as pd
import pytest
from src.transform import (
    safe_json_load,
    clean_students,
    clean_jobs,
    clean_courses,
    validate_foreign_keys,
)


@pytest.mark.parametrize("value", [
    None,
    pd.NA,
    float("nan"),
    {"a": 1},
    '{"a": 1}',
    "bad-json",
    123,
])
def test_safe_json_load(value):
    result = safe_json_load(value)
    assert result in (None, {"a": 1})



def test_clean_students_types():
    raw = pd.DataFrame({
        "job_id": ["1", "x"],
        "num_course_taken": ["2", None],
        "current_career_path_id": ["10", "bad"],
        "time_spent_hrs": ["12.5", "oops"],
        "dob": ["1990-01-01", "invalid"],
        "contact_info": [
            '{"email": "a@test.com"}',
            "bad_json"
        ]
    })

    cleaned = clean_students(raw)

    assert str(cleaned["job_id"].dtype) == "Int64"
    assert cleaned["time_spent_hrs"].dtype == "float64"
    assert pd.api.types.is_datetime64_any_dtype(cleaned["dob"])
    assert "email" in cleaned.columns


def test_clean_jobs_salary_float():
    raw = pd.DataFrame({
        "job_id": ["1", "2"],
        "avg_salary": ["50000", "bad"]
    })

    cleaned = clean_jobs(raw)

    assert cleaned["avg_salary"].dtype == "float64"
    assert cleaned["avg_salary"].isna().sum() == 1


def test_clean_courses_types():
    raw = pd.DataFrame({
        "career_path_id": ["1", "bad"],
        "hours_to_complete": ["100", "oops"]
    })

    cleaned = clean_courses(raw)

    assert str(cleaned["career_path_id"].dtype) == "Int64"
    assert cleaned["hours_to_complete"].dtype == "float64"


def test_validate_foreign_keys_does_not_raise():
    students = pd.DataFrame({
        "job_id": pd.Series([1, 2, None], dtype="Int64"),
        "current_career_path_id": pd.Series([10, None, 20], dtype="Int64")
    })

    jobs = pd.DataFrame({
        "job_id": pd.Series([1, 2], dtype="Int64")
    })

    courses = pd.DataFrame({
        "career_path_id": pd.Series([10], dtype="Int64")
    })

    # Should only log warnings, not raise
    validate_foreign_keys(students, jobs, courses)
