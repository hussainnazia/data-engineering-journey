# API to PostgreSQL ETL Pipeline

## Overview

An end-to-end ETL pipeline built with Python that extracts data from a REST API, validates and transforms the data, and loads it into PostgreSQL.

The pipeline also includes incremental loading, rejected-record handling, ETL audit logging, and error handling.

## Architecture

`REST API → Extract → Transform → Validate → PostgreSQL`

## Technologies

* Python
* REST API
* Requests
* PostgreSQL
* Psycopg2
* python-dotenv
* Logging

## Pipeline Features

### 1. Extract

Retrieves user data from a REST API using Python's `requests` library.

* HTTP status validation
* Request timeout
* API error handling

### 2. Transform & Validate

Transforms the API response into a database-ready structure.

* Extracts required fields
* Validates required data
* Handles missing or invalid fields
* Separates clean and rejected records

### 3. Load

Loads clean records into PostgreSQL.

The pipeline supports:

* Insert
* Update
* Skip unchanged records
* Upsert logic

### 4. Incremental Loading

The pipeline tracks the last successful execution using a watermark.

Only records requiring changes are processed on subsequent runs.

### 5. Rejected Records

Invalid records are separated from valid records and stored for further investigation instead of stopping the entire pipeline.

### 6. ETL Audit Logging

Each pipeline execution is recorded with:

* Run ID
* Start time
* End time
* Status
* Extracted record count
* Inserted record count
* Updated record count
* Skipped record count
* Rejected record count
* Error information

## Database Tables

### `api_users`

Stores cleaned user data extracted from the API.

### `rejected_users`

Stores records that fail validation.

### `etl_control`

Stores the watermark / last successful pipeline execution.

### `etl_runs`

Stores the audit history of pipeline executions.

## Project Structure

```text
03-api-postgres-etl/
│
├── api_to_postgresql.py
├── README.md
├── .gitignore
└── .env
```

> `.env` contains local database credentials and is intentionally excluded from Git.

## What I Practiced

* Building an end-to-end ETL pipeline
* Working with REST APIs
* Python exception handling
* Data validation
* PostgreSQL connectivity
* Upsert / incremental loading
* Watermark management
* Rejected-record processing
* ETL audit logging
* Environment variable management
* Git and GitHub workflow

## Future Improvements

* Add automated tests
* Add configuration management
* Containerize with Docker
* Add workflow orchestration
* Add cloud storage
* Deploy the pipeline to a cloud environment

