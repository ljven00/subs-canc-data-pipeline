# Project Write-Up: Subscription Cancellations Data Pipeline

## Problem Context

Operational databases are often not suitable for analytics without cleaning and standardization. This project simulates a real-world scenario in which customer data is ingested from multiple sources into a SQLite database with inconsistent types, embedded JSON fields, and incomplete relationships.

The objective was to design a pipeline that could routinely transform this raw data into a clean, analytics-ready dataset with minimal manual intervention.

---

## Design Decisions

### Separation of Concerns

The pipeline is split into distinct layers:
- **Extract**: Data access only
- **Transform**: Cleaning and normalization
- **Validate**: Referential integrity checks
- **Load**: Persistence to analytics storage

This separation improves testability, maintainability, and clarity.

---

### Use of Pandas Nullable Types

Nullable integer columns use Pandas `Int64` instead of standard NumPy integers.
This allows missing values to be represented without coercion errors while
maintaining type safety.

---

### Handling of Null Identifiers

Business rules were explicitly defined:
- Students may have null job or career path identifiers
- Students must always have a UUID
- Courses must always have a career path ID
- Jobs must always have a job ID

Rows violating non-nullable identifier rules are dropped and logged.

---

### JSON Parsing Strategy

The `contact_info` column contains JSON stored as strings. A defensive parsing function (`safe_json_load`) was implemented to:
- Safely handle nulls
- Accept already-parsed dictionaries
- Log malformed or unexpected values without failing the pipeline

---

### Validation Strategy

Foreign key relationships are validated after cleaning. Instead of raising
exceptions, violations are logged. This reflects real-world analytics pipelines where imperfect data should not halt processing but must be observable.

---

### Logging Over Exceptions

Logging is used extensively to:
- Track dropped rows
- Record data quality issues
- Provide auditability

This allows the pipeline to be robust while preserving visibility into problems.

---

### Testing Approach

Unit tests focus on:
- Deterministic transformation behavior
- Schema enforcement
- Load-step smoke testing

SQLite internals are not tested, as the focus is on pipeline correctness rather than database implementation.

---

## Conclusion

This project demonstrates a practical approach to building a production-style data pipeline, balancing robustness, clarity, and maintainability. The resulting system produces a reliable analytics dataset while gracefully handling real-world data imperfections.