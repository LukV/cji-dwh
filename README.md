# Monorepo for Dagster, FastAPI, and Streamlit Integration

## Structure
- `/dagster_pipeline`: Contains the Dagster pipeline to ingest data from an API into DuckDB.
- `/fastapi_service`: Exposes the ingested data via a FastAPI service.
- `/streamlit_app`: Fetches data from FastAPI and displays it using Streamlit.
- `/data`: Contains the shared DuckDB database.

## How to Run
1. **Run Dagster Pipeline:**
   ```bash
   cd dagster_pipeline
   dagster dev
   ```

2. Run FastAPI Service:
   ```bash
   cd fastapi_service
   uvicorn src.main:app --reload
   ```

3. Run Streamlit App:
   ```bash
   cd streamlit_app
   streamlit run src/app.py
   ```
