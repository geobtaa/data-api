import asyncio

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from app.main import app
from db.database import database


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest_asyncio.fixture(scope="function", autouse=True)
async def setup_database():
    """Initialize database connection for tests."""
    try:
        if not database.is_connected:
            await database.connect()
        yield
    finally:
        if database.is_connected:
            await database.disconnect()


@pytest.fixture
def test_client():
    """Create a test client."""
    with TestClient(app) as client:
        yield client


def test_application_startup():
    """Test that the application starts without errors."""
    client = TestClient(app)
    response = client.get("/api/v1")
    assert response.status_code == 200


def test_api_docs_available():
    """Test that the API documentation is available."""
    client = TestClient(app)
    response = client.get("/docs")
    assert response.status_code == 200
    assert "swagger" in response.text.lower()


def test_redoc_available():
    """Test that the ReDoc documentation is available."""
    client = TestClient(app)
    response = client.get("/redoc")
    assert response.status_code == 200
    assert "redoc" in response.text.lower()


def test_api_version():
    """Test that the API root returns a response with version info."""
    client = TestClient(app)
    response = client.get("/api/v1")
    assert response.status_code == 200
    data = response.json()
    assert "version" in data
    assert "api" in data
    assert data["api"] == "BTAA Geodata API"
