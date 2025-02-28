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

class IIIFDownloadService:
    """Service for generating IIIF image download options."""
    
    # Standard IIIF image sizes
    SIZES = {
        'thumb': {'width': 150, 'height': 150},
        'small': {'width': 800, 'height': 800},
        'medium': {'width': 1200, 'height': 1200},
        'large': {'width': 2000, 'height': 2000}
    }

    def __init__(self, references: Dict):
        """Initialize with document references."""
        self.image_api_endpoint = references.get('http://iiif.io/api/image')
        self.manifest_url = references.get('http://iiif.io/api/presentation#manifest')

    def get_download_options(self) -> List[Dict]:
        """Generate download options for IIIF images."""
        if not self.image_api_endpoint:
            return []

        # Remove /info.json from endpoint if present
        base_url = self.image_api_endpoint.replace('/info.json', '')
        
        downloads = []

        # Add standard size options
        for size_name, dimensions in self.SIZES.items():
            downloads.append({
                'label': f'{size_name.title()} Image',
                'url': f'{base_url}/full/{dimensions["width"]},{dimensions["height"]}/0/default.jpg',
                'type': 'image/jpeg'
            })

        # Add full size option
        downloads.append({
            'label': 'Full Resolution Image',
            'url': f'{base_url}/full/full/0/default.jpg',
            'type': 'image/jpeg'
        })

        return downloads

class DownloadService:
    """Service for generating download options for documents."""

    def __init__(self, document: Dict):
        """Initialize with document."""
        self.document = document
        self.wxs_identifier = document.get("gbl_wxsidentifier_s", "")
        self.references = self._parse_references()

    def _parse_references(self) -> Dict:
        """Parse references from document."""
        refs = self.document.get('dct_references_s', {})
        if isinstance(refs, str):
            try:
                return json.loads(refs)
            except json.JSONDecodeError:
                return {}
        return refs

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
        """Get all available download options."""
        downloads = []

        # Check for IIIF image API
        if 'http://iiif.io/api/image' in self.references:
            iiif_service = IIIFDownloadService(self.references)
            downloads.extend(iiif_service.get_download_options())

        # Check for direct download URL
        if download_url := self.references.get('http://schema.org/downloadUrl'):
            downloads.append({
                'label': f'Download {self.document.get("dct_format_s", "File")}',
                'url': download_url,
                'type': self.document.get('dct_format_s', 'application/octet-stream').lower()
            })

        return downloads 