from fastapi import FastAPI
from .routers import infrastructure

app = FastAPI(
    title="Infrastructure API",
    description="API for accessing infrastructure data from a read-only DuckDB table.",
    version="1.0.0"
)

app.include_router(infrastructure.router, prefix="/api", tags=["infrastructures"])
