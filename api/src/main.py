import os
from dotenv import load_dotenv
from fastapi import FastAPI
from .routers import infrastructure

# Attempt to load environment variables from .env if it exists
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'), override=True)

app = FastAPI(
    title="Cultuur- en Jeugdinfrastructuur API",
    description="API for accessing culture and youth infrastructure data collected from open data sources managed by the Flemish government.",
    version="1.0.0"
)

app.include_router(infrastructure.router, prefix="/api", tags=["infrastructures"])
