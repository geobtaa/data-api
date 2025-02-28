from typing import Dict, List, Optional
import json
import logging
import os
from dataclasses import dataclass
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

@dataclass
class DownloadOption:
    """Represents a download option with its parameters."""
    label: str
    type: str
    extension: str
    service_type: str
    content_type: str
    request_params: Dict
    reflect: bool = False

class DownloadService:
    """Service for generating download links."""

    def __init__(self, document: Dict):
        self.document = document
        self.wxs_identifier = document.get("gbl_wxsidentifier_s", "")
        self.references = self._parse_references()

    def _parse_references(self) -> Dict:
        """Parse references from the document."""
        try:
            if refs := self.document.get("dct_references_s"):
                return json.loads(refs) if isinstance(refs, str) else refs
            return {}
        except json.JSONDecodeError:
            return {}

    def _get_direct_downloads(self) -> List[Dict]:
        """Get direct download URLs from schema.org references."""
        downloads = []
        if download_info := self.references.get("http://schema.org/downloadUrl"):
            # Handle list of dictionaries
            if isinstance(download_info, list):
                for item in download_info:
                    if isinstance(item, dict) and 'label' in item and 'url' in item:
                        downloads.append({
                            "label": item["label"],
                            "url": item["url"],
                            "type": "download",
                            "format": self._guess_format(item["url"])
                        })
            # Handle single dictionary
            elif isinstance(download_info, dict) and 'label' in download_info and 'url' in download_info:
                downloads.append({
                    "label": download_info["label"],
                    "url": download_info["url"],
                    "type": "download",
                    "format": self._guess_format(download_info["url"])
                })
            # Handle direct URL string
            elif isinstance(download_info, str):
                # Create a descriptive label based on the format
                format_type = self._guess_format(download_info)
                label = f"Download {format_type.upper()}"
                downloads.append({
                    "label": label,
                    "url": download_info,
                    "type": "download",
                    "format": format_type
                })

        return downloads

    def _guess_format(self, url: str) -> str:
        """Guess the format from the URL."""
        if url.lower().endswith('.zip'):
            return 'zip'
        elif url.lower().endswith('.pdf'):
            return 'pdf'
        elif url.lower().endswith('.tif') or url.lower().endswith('.tiff'):
            return 'tiff'
        elif url.lower().endswith('.json'):
            return 'json'
        return 'unknown'

    def _get_service_url(self, service_type: str) -> Optional[str]:
        """Get the endpoint URL for a specific service type."""
        service_map = {
            'wfs': 'http://www.opengis.net/def/serviceType/ogc/wfs',
            'wms': 'http://www.opengis.net/def/serviceType/ogc/wms'
        }
        return self.references.get(service_map.get(service_type))

    def _build_download_url(self, option: DownloadOption) -> Optional[str]:
        """Build the download URL with parameters."""
        base_url = self._get_service_url(option.service_type)
        if not base_url:
            return None

        url = f"{base_url}/reflect" if option.reflect else base_url
        return f"{url}?{urlencode(option.request_params)}"

    def get_download_options(self) -> List[Dict]:
        """Get all available download options for the document."""
        options = []
        
        # Add direct downloads first
        options.extend(self._get_direct_downloads())
        
        # Add service-based downloads (WFS, WMS, etc.)
        if self.wxs_identifier:
            # Shapefile Download Option
            if self._get_service_url('wfs'):
                shapefile_params = {
                    'service': 'wfs',
                    'version': '2.0.0',
                    'request': 'GetFeature',
                    'srsName': 'EPSG:4326',
                    'outputformat': 'SHAPE-ZIP',
                    'typeName': self.wxs_identifier
                }
                shapefile = DownloadOption(
                    label="Shapefile",
                    type="shapefile",
                    extension="zip",
                    service_type="wfs",
                    content_type="application/zip",
                    request_params=shapefile_params
                )
                if url := self._build_download_url(shapefile):
                    options.append({
                        "label": shapefile.label,
                        "url": url,
                        "type": shapefile.type,
                        "format": shapefile.extension
                    })

            # GeoJSON Download Option
            if self._get_service_url('wfs'):
                geojson_params = {
                    'service': 'wfs',
                    'version': '2.0.0',
                    'request': 'GetFeature',
                    'srsName': 'EPSG:4326',
                    'outputformat': 'application/json',
                    'typeName': self.wxs_identifier
                }
                geojson = DownloadOption(
                    label="GeoJSON",
                    type="geojson",
                    extension="json",
                    service_type="wfs",
                    content_type="application/json",
                    request_params=geojson_params
                )
                if url := self._build_download_url(geojson):
                    options.append({
                        "label": geojson.label,
                        "url": url,
                        "type": geojson.type,
                        "format": geojson.extension
                    })

            # GeoTIFF Download Option
            if self._get_service_url('wms'):
                geotiff_params = {
                    'service': 'wms',
                    'version': '1.1.0',
                    'request': 'GetMap',
                    'format': 'image/geotiff',
                    'width': 4096,
                    'layers': self.wxs_identifier
                }
                geotiff = DownloadOption(
                    label="GeoTIFF",
                    type="geotiff",
                    extension="tif",
                    service_type="wms",
                    content_type="image/geotiff",
                    request_params=geotiff_params,
                    reflect=True
                )
                if url := self._build_download_url(geotiff):
                    options.append({
                        "label": geotiff.label,
                        "url": url,
                        "type": geotiff.type,
                        "format": geotiff.extension
                    })

        return options 