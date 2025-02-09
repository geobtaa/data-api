import os
import logging
from typing import Dict, Optional
import json
import requests
from functools import lru_cache
import aiohttp
import asyncio


class ImageService:
    def __init__(self, document: Dict):
        self.document = document

        # Setup logging
        self.logger = logging.getLogger("ImageService")
        log_path = os.getenv("LOG_PATH", "logs")
        os.makedirs(log_path, exist_ok=True)
        log_handler = logging.FileHandler(os.path.join(log_path, "image_service.log"))
        log_handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.INFO)

        # print(f"Document WXS: {self.document.get('gbl_wxsidentifier_s')}")

    @lru_cache(maxsize=1000)
    def _get_cached_manifest(self, manifest_url: str) -> Optional[Dict]:
        """Cache manifest responses to avoid repeated HTTP requests."""
        try:
            response = requests.get(manifest_url, timeout=2.0)  # Add timeout
            response.raise_for_status()
            return response.json()
        except Exception as e:
            self.logger.error(f"Error fetching manifest {manifest_url}: {e}")
            return None

    def get_iiif_manifest_thumbnail(self, manifest_url: str) -> Optional[str]:
        """
        Get thumbnail URL from IIIF Manifest.

        Args:
            manifest_url (str): URL to the IIIF manifest

        Returns:
            Optional[str]: Thumbnail URL or None if not found
        """
        try:
            response = requests.get(manifest_url)
            response.raise_for_status()
            manifest_json = response.json()

            # Sequences - Return the first image if it exists
            if manifest_json.get("sequences"):
                self.logger.debug("Image: sequences")
                canvas = manifest_json.get("sequences", [{}])[0].get("canvases", [{}])[0]
                image = canvas.get("images", [{}])[0].get("resource", {})

                # Handle OSU variant
                if image.get("@id", "").find("osu") != -1:
                    self.logger.debug("Image: sequences - OSU variant")
                    service_id = image.get("service", {}).get("@id")
                    if service_id:
                        return f"{service_id}/full/400,/0/default.jpg"

                # Standard sequence image
                if image.get("@id"):
                    return image["@id"]

            # Items - Northwestern style
            elif manifest_json.get("items"):
                items_path = (
                    manifest_json.get("items", [{}])[0].get("items", [{}])[0].get("items", [{}])[0]
                )

                # Try body.id first
                if items_path.get("body", {}).get("id"):
                    self.logger.debug("Image: items body id")
                    return items_path["body"]["id"]

                # Try direct id
                elif items_path.get("id"):
                    self.logger.debug("Image: items id")
                    return items_path["id"]

            # Thumbnail - Try various thumbnail formats
            elif manifest_json.get("thumbnail"):
                self.logger.debug("Image: thumbnail")
                thumbnail = manifest_json["thumbnail"]
                if isinstance(thumbnail, dict):
                    return thumbnail.get("@id") or thumbnail.get("id")
                return thumbnail

            # Fallback to viewer endpoint
            self.logger.debug("Image: failed to find thumbnail")
            return manifest_url

        except Exception as e:
            self.logger.error(f"Error processing IIIF manifest: {e}")
            return manifest_url

    def get_thumbnail_url(self) -> Optional[str]:
        """Get the appropriate thumbnail URL based on the document type."""
        try:
            references = self.document.get("dct_references_s")
            if isinstance(references, str):
                try:
                    references = json.loads(references)
                except json.JSONDecodeError:
                    return None

            if not isinstance(references, dict):
                return None

            # Direct thumbnail URL - fastest path
            if "http://schema.org/thumbnailUrl" in references:
                url = references["http://schema.org/thumbnailUrl"]
                return url.replace("/full/full/0/", "/full/400,/0/") if "/full/full/0/" in url else url

            # IIIF Image API - fast path
            if "http://iiif.io/api/image" in references:
                viewer_endpoint = references["http://iiif.io/api/image"]
                return f"{viewer_endpoint.replace('info.json', '')}full/400,/0/default.jpg"

            # IIIF Manifest - slower path
            if (
                "https://iiif.io/api/presentation/2/context.json" in references
                or "http://iiif.io/api/presentation#manifest" in references
            ):
                manifest_url = references.get(
                    "https://iiif.io/api/presentation/2/context.json"
                ) or references.get("http://iiif.io/api/presentation#manifest")
                
                # Use cached manifest
                manifest_json = self._get_cached_manifest(manifest_url)
                if manifest_json:
                    # Extract thumbnail URL using existing logic...
                    if "sequences" in manifest_json:
                        canvas = manifest_json.get("sequences", [{}])[0].get("canvases", [{}])[0]
                        image = canvas.get("images", [{}])[0].get("resource", {})
                        
                        if "@id" in image and "/full/full/0/" in image["@id"]:
                            return image["@id"].replace("/full/full/0/", "/full/400,/0/")
                        return image.get("@id")

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
                url = (
                    f"{wms_endpoint}/reflect?"
                    f"FORMAT=image/png&"
                    f"TRANSPARENT=TRUE&"
                    f"WIDTH={width}&"
                    f"HEIGHT={height}&"
                    f"LAYERS={layers}"
                )
                return url.replace("/full/full/0/", "/full/400,/0/") if "/full/full/0/" in url else url

            # Check for TMS
            elif "http://www.opengis.net/def/serviceType/ogc/tms" in references:
                tms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/tms"]
                return f"{tms_endpoint}/reflect?format=application/vnd.google-earth.kml+xml"

        except Exception as e:
            self.logger.error(f"Error getting thumbnail URL for {self.document.get('id')}: {e}")
            return None

        return None
