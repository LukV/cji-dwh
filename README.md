# Monorepo for Dagster, FastAPI, and Streamlit Integration

## Overview
This monorepo integrates three components—Dagster, FastAPI, and Streamlit—to create a data pipeline that ingests, processes, and visualizes data. The ingested data is stored in **Parquet** format on **Amazon S3**, providing centralized storage for efficient access and retrieval across the services.

## Structure
- `/pipeline`: Contains the Dagster pipeline that ingests data from an external API into **Parquet** files, processes them with **DuckDB** and uploads them to **S3**.
- `/api`: Exposes the ingested data stored on **S3** via a **FastAPI** service, allowing for easy access and querying of the data, also using the **DuckDB** engine.
- `/app`: Fetches data from the **FastAPI** service and displays it using **Streamlit** for visualization and interactive analysis.

## How to Run
1. **Run Dagster Pipeline:**
   The Dagster pipeline ingests data and stores it as a DuckDB database and Parquet files on S3.
   ```bash
   cd pipeline
   dagster dev
   ```

2. **Run FastAPI Service:**
   The FastAPI service retrieves data directly from the **DuckDB** database and **Parquet** files stored on **S3**.
   ```bash
   cd api
   uvicorn src.main:app --reload
   ```

3. **Run Streamlit App:**
   The Streamlit app interacts with the **FastAPI** service to display the data fetched from **S3**.
   ```bash
   cd app
   streamlit run src/app.py
   ```