# Subscription Cancellations Data Pipeline

## Overview

This project implements an end-to-end data engineering pipeline that extracts,
cleans, validates, and loads customer-related data from a messy SQLite database
into a clean, analytics-ready format.

The pipeline is designed to simulate a real-world scenario where data is ingested
from multiple sources, inconsistently typed, and partially malformed. The goal
is to produce a reliable source of truth for downstream analytics with minimal
human intervention.

The dataset represents long-term cancelled subscribers for a fictional online
education company called **Cademycode**.

---

## Project Objectives

- Build a production-style ETL pipeline using Python
- Practice data cleaning, validation, and schema enforcement
- Apply logging and unit testing to data transformations
- Automate execution using Bash and PowerShell scripts
- Produce a clean analytics database from raw operational data

---

## Tech Stack

- **Python 3.10**
- **Pandas**
- **SQLite**
- **pytest**
- **Conda**
- **Bash / PowerShell**
- **Jupyter Notebook** (exploration only)

---

## Project Structure

```
subs-canc-data-pipeline/
│
├── data/
│ ├── raw/ # Raw SQLite database (source)
│ └── processed/ # Analytics database (generated, not tracked)
│
├── notebooks/
│ └── 01_exploration_and_cleaning.ipynb
│
├── src/
│ ├── init.py
│ ├── extract.py
│ ├── transform.py
│ ├── load.py
│ ├── run_pipeline.py
│ └── logging_config.py
│
├── tests/
│ ├── test_transform.py
│ └── test_load.py
│
├── scripts/
│ ├── run_pipeline.sh
│ └── run_pipeline.ps1
│
├── environment.yml
├── requirements.txt
├── pyproject.toml
├── README.md
└── WRITEUP.md
```

---


---

## Pipeline Stages

### 1. Extract

- Reads data from a raw SQLite database
- Loads tables into Pandas DataFrames
- Isolated from transformation logic

### 2. Transform

- Enforces correct data types
- Handles nullable and non-nullable identifiers
- Safely parses embedded JSON fields
- Drops invalid rows based on business rules
- Logs all anomalies without failing the pipeline

### 3. Validate

- Verifies foreign key relationships across datasets
- Logs referential integrity issues for auditability

### 4. Load

- Writes cleaned data to a new SQLite analytics database
- Uses idempotent table replacement
- Executes inside a transaction

---

## How to Run the Pipeline

### 1. Create and activate the environment

```bash
conda env create -f environment.yml
conda activate subs-canc
```

### 2. Run the pipeline

Windows (PowerShell)
```
.\scripts\run_pipeline.ps1
```

macOS/Linux/WSL
```
./scripts/run_pipeline.sh
```

## Testing

Run all tests using:
```
pytest
```

Tests cover:

- Data transformation logic
- Schema enforcement
- Load step smoke testing

## Notes

- Processed data and generated databases are intentionally excluded from version control
- Logging is used instead of exceptions for data quality issues to ensure pipeline robustness
- Jupyter notebooks are used strictly for exploration, not production logic