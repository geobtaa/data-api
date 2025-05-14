import json

import pytest

from app.services.download_service import DownloadOption, DownloadService, IIIFDownloadService


@pytest.fixture
def mock_item_with_iiif():
    """Return a mock item with IIIF references."""
    return {
        "id": "test-item-iiif",
        "dct_title_s": "Test IIIF Item",
        "dct_format_s": "JPEG",
        "dct_references_s": json.dumps(
            {
                "http://iiif.io/api/image": "https://example.com/iiif/image/info.json",
                "http://iiif.io/api/presentation#manifest": "https://example.com/iiif/manifest",
            }
        ),
    }


@pytest.fixture
def mock_item_with_direct_download():
    """Return a mock item with direct download URL."""
    return {
        "id": "test-item-download",
        "dct_title_s": "Test Download Item",
        "dct_format_s": "PDF",
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": "https://example.com/download/document.pdf",
            }
        ),
    }


@pytest.fixture
def mock_item_with_download_info_list():
    """Return a mock item with list of download info objects."""
    return {
        "id": "test-item-download-list",
        "dct_title_s": "Test Multiple Downloads Item",
        "dct_format_s": "Mixed",
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": [
                    {"label": "PDF Version", "url": "https://example.com/download/document.pdf"},
                    {"label": "ZIP Archive", "url": "https://example.com/download/data.zip"},
                ],
            }
        ),
    }


@pytest.fixture
def mock_item_with_download_info_dict():
    """Return a mock item with download info as dictionary."""
    return {
        "id": "test-item-download-dict",
        "dct_title_s": "Test Single Download Info Item",
        "dct_format_s": "TIFF",
        "dct_references_s": json.dumps(
            {
                "http://schema.org/downloadUrl": {
                    "label": "High Resolution Image",
                    "url": "https://example.com/download/image.tiff",
                },
            }
        ),
    }


@pytest.fixture
def mock_item_with_service():
    """Return a mock item with WMS/WFS services."""
    return {
        "id": "test-item-service",
        "dct_title_s": "Test Service Item",
        "gbl_wxsidentifier_s": "test_layer",
        "dct_references_s": json.dumps(
            {
                "http://www.opengis.net/def/serviceType/ogc/wms": "https://example.com/geoserver/wms",
                "http://www.opengis.net/def/serviceType/ogc/wfs": "https://example.com/geoserver/wfs",
            }
        ),
    }


class TestIIIFDownloadService:
    """Test cases for IIIFDownloadService."""

    def test_init(self):
        """Test initialization with references."""
        references = {
            "http://iiif.io/api/image": "https://example.com/iiif/image/info.json",
            "http://iiif.io/api/presentation#manifest": "https://example.com/iiif/manifest",
        }
        service = IIIFDownloadService(references)

        assert service.image_api_endpoint == "https://example.com/iiif/image/info.json"
        assert service.manifest_url == "https://example.com/iiif/manifest"

    def test_get_download_options(self):
        """Test generating download options for IIIF images."""
        references = {"http://iiif.io/api/image": "https://example.com/iiif/image/info.json"}
        service = IIIFDownloadService(references)
        options = service.get_download_options()

        # Should have options for thumb, small, medium, large, and full
        assert len(options) == 5

        # Verify thumb option
        thumb = next(opt for opt in options if opt["label"] == "Thumb Image")
        assert thumb["url"] == "https://example.com/iiif/image/full/150,150/0/default.jpg"
        assert thumb["type"] == "image/jpeg"

        # Verify full option
        full = next(opt for opt in options if opt["label"] == "Full Resolution Image")
        assert full["url"] == "https://example.com/iiif/image/full/full/0/default.jpg"
        assert full["type"] == "image/jpeg"

    def test_no_image_endpoint(self):
        """Test behavior when no image endpoint is available."""
        service = IIIFDownloadService({})
        options = service.get_download_options()
        assert options == []


class TestDownloadService:
    """Test cases for DownloadService."""

    def test_parse_references(self, mock_item_with_iiif):
        """Test parsing references from item."""
        service = DownloadService(mock_item_with_iiif)
        refs = service._parse_references()

        assert "http://iiif.io/api/image" in refs
        assert refs["http://iiif.io/api/image"] == "https://example.com/iiif/image/info.json"

    def test_parse_references_with_invalid_json(self):
        """Test parsing references with invalid JSON."""
        doc = {"dct_references_s": "{invalid json}"}
        service = DownloadService(doc)
        refs = service._parse_references()

        assert refs == {}

    def test_get_direct_downloads_url_string(self, mock_item_with_direct_download):
        """Test getting direct download URL as string."""
        service = DownloadService(mock_item_with_direct_download)
        downloads = service._get_direct_downloads()

        assert len(downloads) == 1
        assert downloads[0]["label"] == "Download PDF"
        assert downloads[0]["url"] == "https://example.com/download/document.pdf"
        assert downloads[0]["format"] == "pdf"

    def test_get_direct_downloads_list(self, mock_item_with_download_info_list):
        """Test getting direct downloads as list."""
        service = DownloadService(mock_item_with_download_info_list)
        downloads = service._get_direct_downloads()

        assert len(downloads) == 2
        assert downloads[0]["label"] == "PDF Version"
        assert downloads[0]["format"] == "pdf"
        assert downloads[1]["label"] == "ZIP Archive"
        assert downloads[1]["format"] == "zip"

    def test_get_direct_downloads_dict(self, mock_item_with_download_info_dict):
        """Test getting direct downloads as dict."""
        service = DownloadService(mock_item_with_download_info_dict)
        downloads = service._get_direct_downloads()

        assert len(downloads) == 1
        assert downloads[0]["label"] == "High Resolution Image"
        assert downloads[0]["format"] == "tiff"

    def test_guess_format(self):
        """Test guessing format from URL."""
        service = DownloadService({})

        assert service._guess_format("file.pdf") == "pdf"
        assert service._guess_format("data.zip") == "zip"
        assert service._guess_format("image.tiff") == "tiff"
        assert service._guess_format("data.json") == "json"
        assert service._guess_format("unknown.xyz") == "unknown"

    def test_get_service_url(self, mock_item_with_service):
        """Test getting service URL by type."""
        service = DownloadService(mock_item_with_service)

        wms_url = service._get_service_url("wms")
        assert wms_url == "https://example.com/geoserver/wms"

        wfs_url = service._get_service_url("wfs")
        assert wfs_url == "https://example.com/geoserver/wfs"

        # Test with unsupported service type
        unknown_url = service._get_service_url("unknown")
        assert unknown_url is None

    def test_build_download_url(self, mock_item_with_service):
        """Test building download URL with parameters."""
        service = DownloadService(mock_item_with_service)

        # Test WMS GetMap option
        wms_option = DownloadOption(
            label="WMS Preview",
            type="image",
            extension="png",
            service_type="wms",
            content_type="image/png",
            request_params={
                "SERVICE": "WMS",
                "VERSION": "1.1.1",
                "REQUEST": "GetMap",
                "LAYERS": "test_layer",
                "WIDTH": 800,
                "HEIGHT": 600,
                "FORMAT": "image/png",
                "SRS": "EPSG:4326",
                "BBOX": "-180,-90,180,90",
            },
            reflect=False,
        )

        url = service._build_download_url(wms_option)
        assert "https://example.com/geoserver/wms" in url
        assert "SERVICE=WMS" in url
        assert "REQUEST=GetMap" in url
        assert "LAYERS=test_layer" in url

        # Test with reflect option
        reflect_option = DownloadOption(
            label="WFS GeoJSON",
            type="data",
            extension="json",
            service_type="wfs",
            content_type="application/json",
            request_params={
                "SERVICE": "WFS",
                "VERSION": "2.0.0",
                "REQUEST": "GetFeature",
                "TYPENAME": "test_layer",
                "OUTPUTFORMAT": "application/json",
            },
            reflect=True,
        )

        url = service._build_download_url(reflect_option)
        assert "https://example.com/geoserver/wfs/reflect" in url
        assert "SERVICE=WFS" in url
        assert "REQUEST=GetFeature" in url

    def test_get_download_options_iiif(self, mock_item_with_iiif):
        """Test getting download options for IIIF item."""
        service = DownloadService(mock_item_with_iiif)
        options = service.get_download_options()

        # Should have options from IIIF service
        assert len(options) == 5

        # Verify thumb and full options exist
        labels = [opt["label"] for opt in options]
        assert "Thumb Image" in labels
        assert "Full Resolution Image" in labels

    def test_get_download_options_direct(self, mock_item_with_direct_download):
        """Test getting download options for direct download item."""
        service = DownloadService(mock_item_with_direct_download)
        options = service.get_download_options()

        assert len(options) == 1
        assert options[0]["label"] == "Download PDF"
        assert options[0]["url"] == "https://example.com/download/document.pdf"
        assert options[0]["type"] == "pdf"
