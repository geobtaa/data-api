from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


@pytest.fixture
def mock_geonames_record():
    """Return a mock GeoNames record for testing."""
    return {
        "geonameid": 123456,
        "name": "Test Location",
        "asciiname": "Test Location",
        "alternatenames": "Test Alt Name",
        "latitude": 44.9778,
        "longitude": -93.2650,
        "feature_class": "A",
        "feature_code": "ADM1",
        "country_code": "US",
        "cc2": "",
        "admin1_code": "MN",
        "admin2_code": "123",
        "admin3_code": "",
        "admin4_code": "",
        "population": 500000,
        "elevation": 200,
        "dem": 200,
        "timezone": "America/Chicago",
        "modification_date": datetime(2023, 1, 1),
    }


@pytest.fixture
def mock_wof_record():
    """Return a mock WOF record for testing."""
    return {
        "wok_id": 123456,
        "name": "Test WOF Location",
        "placetype": "region",
        "country": "US",
        "parent_id": 12345,
        "latitude": 44.9778,
        "longitude": -93.2650,
        "min_latitude": 44.0,
        "min_longitude": -94.0,
        "max_latitude": 45.0,
        "max_longitude": -93.0,
        "is_current": 1,
        "is_deprecated": 0,
        "is_ceased": 0,
        "is_superseded": 0,
        "is_superseding": 0,
        "repo": "whosonfirst-data",
        "lastmodified": 12345678,
    }


@pytest.fixture
def mock_btaa_record():
    """Return a mock BTAA record for testing."""
    return {
        "id": 123,
        "fast_area": "Minnesota",
        "bounding_box": "ENVELOPE(-97.2, -89.5, 49.0, 43.5)",
        "geometry": None,
        "geonames_id": "5037779",
        "state_abbv": "MN",
        "state_name": "Minnesota",
        "county_fips": None,
        "statefp": "27",
        "namelsad": "Minnesota",
    }


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.database.fetch_val")
async def test_list_gazetteers(mock_fetch_val):
    """Test the list_gazetteers endpoint."""
    # Setup mocks
    mock_fetch_val.side_effect = [500, 200, 100, 50, 40, 30, 20]  # Different counts for tables

    # Call endpoint
    response = client.get("/api/v1/gazetteers")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 3  # 3 gazetteers

    # Verify gazetteer data
    geonames = next(g for g in data["data"] if g["id"] == "geonames")
    wof = next(g for g in data["data"] if g["id"] == "wof")
    btaa = next(g for g in data["data"] if g["id"] == "btaa")

    assert geonames["attributes"]["name"] == "GeoNames"
    assert geonames["attributes"]["record_count"] == 500

    assert wof["attributes"]["name"] == "Who's on First"
    assert wof["attributes"]["record_count"] == 200
    assert "additional_tables" in wof["attributes"]

    assert btaa["attributes"]["name"] == "BTAA"
    assert btaa["attributes"]["record_count"] == 100


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.database.fetch_all")
@patch("app.api.v1.gazetteer.database.fetch_val")
async def test_search_geonames(mock_fetch_val, mock_fetch_all, mock_geonames_record):
    """Test the search_geonames endpoint."""
    # Setup mocks
    mock_fetch_all.return_value = [mock_geonames_record]
    mock_fetch_val.return_value = 1

    # Call endpoint with query params
    response = client.get("/api/v1/gazetteers/geonames?q=Test&country_code=US&limit=10")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 1
    assert data["data"][0]["type"] == "geoname"
    assert data["data"][0]["id"] == str(mock_geonames_record["geonameid"])
    assert data["data"][0]["attributes"]["name"] == mock_geonames_record["name"]

    # Verify metadata
    assert "meta" in data
    assert data["meta"]["total_count"] == 1
    assert data["meta"]["limit"] == 10
    assert "query" in data["meta"]
    assert data["meta"]["query"]["q"] == "Test"


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.database.fetch_all")
@patch("app.api.v1.gazetteer.database.fetch_val")
async def test_search_wof(mock_fetch_val, mock_fetch_all, mock_wof_record):
    """Test the search_wof endpoint."""
    # Setup mocks
    mock_fetch_all.return_value = [mock_wof_record]
    mock_fetch_val.return_value = 1

    # Call endpoint with query params
    response = client.get("/api/v1/gazetteers/wof?q=Test&country=US&placetype=region&limit=10")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 1
    assert data["data"][0]["type"] == "wof"
    assert data["data"][0]["id"] == str(mock_wof_record["wok_id"])
    assert data["data"][0]["attributes"]["name"] == mock_wof_record["name"]

    # Verify metadata
    assert "meta" in data
    assert data["meta"]["total_count"] == 1
    assert data["meta"]["limit"] == 10
    assert "query" in data["meta"]
    assert data["meta"]["query"]["q"] == "Test"
    assert data["meta"]["query"]["country"] == "US"
    assert data["meta"]["query"]["placetype"] == "region"


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.database.fetch_all")
@patch("app.api.v1.gazetteer.database.fetch_val")
async def test_search_btaa(mock_fetch_val, mock_fetch_all, mock_btaa_record):
    """Test the search_btaa endpoint."""
    # Setup mocks
    mock_fetch_all.return_value = [mock_btaa_record]
    mock_fetch_val.return_value = 1

    # Call endpoint with query params
    response = client.get("/api/v1/gazetteers/btaa?q=Minnesota&state_abbv=MN&limit=10")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) == 1
    assert data["data"][0]["type"] == "btaa"
    assert data["data"][0]["id"] == str(mock_btaa_record["id"])
    assert data["data"][0]["attributes"]["fast_area"] == mock_btaa_record["fast_area"]

    # Verify metadata
    assert "meta" in data
    assert data["meta"]["total_count"] == 1
    assert data["meta"]["limit"] == 10
    assert "query" in data["meta"]
    assert data["meta"]["query"]["q"] == "Minnesota"
    assert data["meta"]["query"]["state_abbv"] == "MN"


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.search_geonames")
@patch("app.api.v1.gazetteer.search_wof")
@patch("app.api.v1.gazetteer.search_btaa")
async def test_search_all_gazetteers(mock_search_btaa, mock_search_wof, mock_search_geonames):
    """Test the search_all_gazetteers endpoint."""
    # Setup mocks
    mock_search_geonames.return_value = {
        "data": [{"id": "geo1", "type": "geoname", "attributes": {"name": "Geo Test"}}],
        "meta": {"total_count": 1},
    }
    mock_search_wof.return_value = {
        "data": [{"id": "wof1", "type": "wof", "attributes": {"name": "WOF Test"}}],
        "meta": {"total_count": 1},
    }
    mock_search_btaa.return_value = {
        "data": [{"id": "btaa1", "type": "btaa", "attributes": {"fast_area": "BTAA Test"}}],
        "meta": {"total_count": 1},
    }

    # Call endpoint
    response = client.get("/api/v1/gazetteers/search?q=Test&country_code=US")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data
    assert len(data["data"]) <= 10  # Default limit is 10

    # Verify all gazetteer types are included
    gazetteer_types = [item["source"] for item in data["data"]]
    assert "geonames" in gazetteer_types
    assert "wof" in gazetteer_types
    assert "btaa" in gazetteer_types

    # Verify that all search methods were called
    mock_search_geonames.assert_called_once()
    mock_search_wof.assert_called_once()
    mock_search_btaa.assert_called_once()


@pytest.mark.asyncio
@patch("app.api.v1.gazetteer.search_geonames")
async def test_search_specific_gazetteer(mock_search_geonames):
    """Test the search_all_gazetteers endpoint with specific gazetteer."""
    # Setup mocks
    mock_search_geonames.return_value = {
        "data": [{"id": "geo1", "type": "geoname", "attributes": {"name": "Geo Test"}}],
        "meta": {"total_count": 1},
    }

    # Call endpoint with specific gazetteer
    response = client.get("/api/v1/gazetteers/search?q=Test&gazetteer=geonames")

    # Verify response
    assert response.status_code == 200
    data = response.json()
    assert "data" in data

    # Verify only the specified gazetteer was searched
    gazetteer_types = [item["source"] for item in data["data"]]
    assert all(g == "geonames" for g in gazetteer_types)

    # Verify that only geonames search was called
    mock_search_geonames.assert_called_once()

    # Verify metadata
    assert data["meta"]["query"]["gazetteer"] == "geonames"
    assert "geonames" in data["meta"]["query"]["gazetteers_searched"]
