from fastapi import FastAPI
from app.api.v1.endpoints import router as api_router
from app.elasticsearch import init_elasticsearch, close_elasticsearch
from db.database import database
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware


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
    allow_origins=["http://localhost:5173"],  # Allow requests from this origin
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

app.include_router(api_router, prefix="/api/v1")
