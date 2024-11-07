import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routers import infrastructure
from .db.database import cache_manager

# Load environment variables from .env if it exists
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'), override=True)

@asynccontextmanager
async def lifespan(app: FastAPI): # pylint: disable=W0621,W0613
    """Load the data into cache on application startup."""
    cache_manager.load_data_into_cache()
    yield

app = FastAPI(
    title="Cultuur- en Jeugdinfrastructuur API",
    description="API for accessing culture and youth infrastructure data \
        collected from open data sources managed by the Flemish government.",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://cji.dataframeone.com", "https://test.cji.dcjm.be/"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(infrastructure.router, prefix="/api", tags=["infrastructures"])
