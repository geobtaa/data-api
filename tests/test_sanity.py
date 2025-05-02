from fastapi.testclient import TestClient

from app.main import app


def test_application_startup():
    """Test that the application starts without errors."""
    client = TestClient(app)
    assert client.app is not None


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
    assert data["api"] == "BTAA Geoportal API"
