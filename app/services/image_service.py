import os
import logging
from typing import Dict, Optional
import json

class ImageService:
    def __init__(self, document: Dict):
        self.document = document
        
        # Setup logging
        self.logger = logging.getLogger("ImageService")
        log_path = os.getenv("LOG_PATH", "logs")
        os.makedirs(log_path, exist_ok=True)
        log_handler = logging.FileHandler(os.path.join(log_path, "image_service.log"))
        log_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.INFO)

        print(f"Document WXS: {self.document.get('gbl_wxsidentifier_s')}")

    def get_thumbnail_url(self) -> Optional[str]:
        """Get the appropriate thumbnail URL based on the document type."""
        try:
            references = self.document.get("dct_references_s")
            if isinstance(references, str):
                try:
                    references = json.loads(references)
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse references JSON for {self.document.get('id')}")
                    return None
            
            if not isinstance(references, dict):
                return None

            # Check for direct thumbnail URL first
            if "http://schema.org/thumbnailUrl" in references:
                return references["http://schema.org/thumbnailUrl"]

            # Check for IIIF
            if "http://iiif.io/api/image" in references:
                viewer_endpoint = references["http://iiif.io/api/image"]
                return viewer_endpoint.replace("info.json", "") + "full/max/0/default.jpg"
            
            # Check for ESRI services
            elif "urn:x-esri:serviceType:ArcGIS#ImageMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#ImageMapLayer"]
                return f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#TiledMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#TiledMapLayer"]
                return f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#DynamicMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#DynamicMapLayer"]
                return f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            
            # Check for WMS
            elif "http://www.opengis.net/def/serviceType/ogc/wms" in references:
                wms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/wms"]
                width = 200
                height = 200
                layers = self.document.get("gbl_wxsidentifier_s", "")
                return (f"{wms_endpoint}/reflect?"
                       f"FORMAT=image/png&"
                       f"TRANSPARENT=TRUE&"
                       f"WIDTH={width}&"
                       f"HEIGHT={height}&"
                       f"LAYERS={layers}")
            
            # Check for TMS
            elif "http://www.opengis.net/def/serviceType/ogc/tms" in references:
                tms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/tms"]
                return f"{tms_endpoint}/reflect?format=application/vnd.google-earth.kml+xml"

        except Exception as e:
            self.logger.error(f"Error getting thumbnail URL for {self.document.get('id')}: {e}")
            return None

        return None