# Monorepo for Dagster, FastAPI, and Streamlit Integration

## Structure
- `/pipeline`: Contains the Dagster pipeline to ingest data from an API into DuckDB.
- `/api`: Exposes the ingested data via a FastAPI service.
- `/app`: Fetches data from FastAPI and displays it using Streamlit.
- `/data`: Contains the shared files and DuckDB database.

## How to Run
1. **Run Dagster Pipeline:**
   ```bash
   cd dagster_pipeline
   dagster dev
   ```

2. **Run FastAPI Service:**
   ```bash
   cd fastapi_service
   uvicorn src.main:app --reload
   ```

3. **Run Streamlit App:**
   ```bash
   cd streamlit_app
   streamlit run src/app.py
   ```
