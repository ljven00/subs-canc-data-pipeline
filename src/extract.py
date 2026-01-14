"""
Docstring for extract
===================================

This file is responsible for only extracting the data
from the database.
"""

import sqlite3
import pandas as pd
from pathlib import Path


DEFAULT_DB_PATH = (
    Path.cwd() 
    / "data"
    / "raw"
    / "cademycode.db"
)


def get_connection(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """
    Create and return a SQLite connection.
    
    :param db_path: SQLite database's path
    :type db_path: str | Path
    :return: Connection to the database
    :rtype: sqlite3.Connection
    """

    db_path = Path(db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"Database not found at: {db_path}")
    
    return sqlite3.connect(db_path)


# -----------------------
# Extraction functions
# -----------------------


def extract_students(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Extract students data from the database. 
    
    :param conn: Open SQLite connection
    :type conn: sqlite3.Connection
    :return: A dataframe containing the students data
    :rtype: pd.DataFrame
    """

    qry = "SELECT * FROM cademycode_students;"
    return pd.read_sql(qry, conn)


def extract_courses(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Extract courses data from the database. 

    :param conn: Open SQLite connection
    :type conn: sqlite3.Connection
    :return: A dataframe containing the courses data
    :rtype: pd.DataFrame
    """

    qry = "SELECT * FROM cademycode_courses;"
    return pd.read_sql(qry, conn)


def extract_jobs(conn: sqlite3.Connection) -> pd.DataFrame:
    """
     Extract jobs data from the database.
    
    :param conn: Open SQLite connection
    :type conn: sqlite3.Connection
    :return: A dataframe containing the jobs data
    :rtype: pd.DataFrame
    """

    qry = "SELECT * FROM cademycode_student_jobs;"
    return pd.read_sql(qry, conn)


def extract_all(db_path: str | Path = DEFAULT_DB_PATH) -> dict[str, pd.DataFrame]:
    """
    Extract all source tables and return them as a dictionary.
    
    :param db_path: SQLite database's path
    :type db_path: str | Path
    :return: A map of table_names to their data
    :rtype: dict[str, DataFrame]
    """

    conn = get_connection(db_path)

    try:
        return {
            "students": extract_students(conn),
            "courses": extract_courses(conn),
            "jobs": extract_jobs(conn),
        }
    finally:
        conn.close()
