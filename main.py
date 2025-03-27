from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import logging
import sys
from app.api.v1.endpoints import router as api_router
from app.api.v1.gazetteer import router as gazetteer_router
from app.elasticsearch import init_elasticsearch, close_elasticsearch
from db.database import database
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
import os

# Create logs directory if it doesn't exist
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/app.log"),  # Write to logs directory
    ],
)
logger = logging.getLogger(__name__)

# Get CORS origins from environment variable
cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event
    await database.connect()
    await init_elasticsearch()
    yield
    # Shutdown event
    await database.disconnect()
    await close_elasticsearch()


app = FastAPI(title="BTAA Geoportal API", version="0.1.0", lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,  # Cache preflight requests for 1 hour
)

# Include routers
app.include_router(api_router, prefix="/api/v1")
app.include_router(gazetteer_router, prefix="/api/v1")


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Global exception handler caught: {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "detail": str(exc),
            "path": str(request.url),
            "method": request.method,
        },
    )
