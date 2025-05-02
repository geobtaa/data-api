import os

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from app.services.cache_service import CacheService, cached_endpoint

# Set environment variable to enable caching for tests
os.environ["ENDPOINT_CACHE"] = "true"

app = FastAPI()
cache_service = CacheService()


@app.get("/test-success")
@cached_endpoint(ttl=60)
async def success_route():
    return {"status": "success"}


@app.get("/test-error")
@cached_endpoint(ttl=60)
async def error_route():
    raise HTTPException(status_code=404, detail="Not found")


client = TestClient(app)


@pytest.mark.asyncio
async def test_success_response_caching():
    # First request - should miss cache
    response1 = client.get("/test-success")
    assert response1.status_code == 200
    assert response1.json() == {"status": "success"}

    # Second request - should hit cache
    response2 = client.get("/test-success")
    assert response2.status_code == 200
    assert response2.json() == {"status": "success"}


@pytest.mark.asyncio
async def test_error_response_not_cached():
    # First request - should return error
    response1 = client.get("/test-error")
    assert response1.status_code == 404
    assert response1.json() == {"detail": "Not found"}

    # Second request - should still return error (not cached)
    response2 = client.get("/test-error")
    assert response2.status_code == 404
    assert response2.json() == {"detail": "Not found"}
