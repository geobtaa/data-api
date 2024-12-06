from fastapi import FastAPI
from app.api.v1.endpoints import router as api_router
from db.database import database
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup event
    await database.connect()
    yield
    # Shutdown event
    await database.disconnect()


app = FastAPI(lifespan=lifespan)
app.include_router(api_router, prefix="/api/v1")
