from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.api.v1.api import api_router_v1
from app.db.database import get_supabase_client
from app.core.config import settings # To ensure settings are loaded

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application startup...")
    get_supabase_client() # Initialize the client on startup
    yield
    # Shutdown
    logger.info("Application shutdown...")
    # No explicit cleanup needed for Supabase client

app = FastAPI(
    title="Incarts Analytics Data API",
    description="API to serve aggregated analytics data from Supabase PostgreSQL.",
    version="1.0.0",
    openapi_url="/api/v1/openapi.json",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan
)

# CORS Configuration
# Adjust origins as needed for your frontend applications
origins = [
    "http://localhost",
    "http://localhost:3000", # Example for a React frontend
    "http://localhost:8080", # Example for a Vue frontend
    # Add your production frontend domains here
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router_v1, prefix="/api/v1")

@app.get("/", tags=["Root"])
async def read_root():
    return {"message": "Welcome to the Analytics Data API. See /docs for API documentation."}

if __name__ == "__main__":
    import uvicorn
    # This is for local development. For production, use a proper ASGI server like Gunicorn with Uvicorn workers.
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)