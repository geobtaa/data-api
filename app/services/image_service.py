import os
import logging
from typing import Dict, Optional
import json
import requests
import aiohttp
import asyncio
import redis
from datetime import timedelta
from app.tasks.worker import fetch_and_cache_image
import hashlib
import re


class ImageService:
    def __init__(self, document: Dict):
        self.document = document

        # Setup Redis connection
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", 6379))
        self.application_url = os.getenv("APPLICATION_URL", "http://localhost:8000").rstrip("/")
        self.cache = redis.Redis(
            host=self.redis_host, port=self.redis_port, db=0, decode_responses=True
        )
        self.cache_ttl = int(os.getenv("REDIS_TTL", 604800))  # 7 days in seconds

        # Setup binary Redis connection for images
        self.image_cache = redis.Redis(
            host=self.redis_host,
            port=self.redis_port,
            db=1,  # Use different DB for images
            decode_responses=False,
        )

        # Setup logging
        self.logger = logging.getLogger("ImageService")
        log_path = os.getenv("LOG_PATH", "logs")
        os.makedirs(log_path, exist_ok=True)
        log_handler = logging.FileHandler(os.path.join(log_path, "image_service.log"))
        log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(log_handler)
        self.logger.setLevel(logging.INFO)

        # print(f"Document WXS: {self.document.get('gbl_wxsidentifier_s')}")

    def _get_manifest(self, manifest_url: str) -> Optional[Dict]:
        """Get manifest from cache or fetch and cache it."""
        cache_key = f"manifest:{manifest_url}"

        # Try to get from cache
        cached_data = self.cache.get(cache_key)
        if cached_data:
            self.logger.info(f"🚀 Cache HIT for manifest {manifest_url}")
            return json.loads(cached_data)

        # If not in cache, fetch and store
        try:
            self.logger.info(f"🐌 Cache MISS for manifest {manifest_url}")
            response = requests.get(manifest_url, timeout=2.0)
            response.raise_for_status()
            manifest_data = response.json()

            # Cache the manifest
            self.cache.setex(cache_key, self.cache_ttl, json.dumps(manifest_data))
            return manifest_data
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
        manifest_json = self._get_manifest(manifest_url)
        if not manifest_json:
            return manifest_url

        try:
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

    def _standardize_iiif_url(self, url: str) -> str:
        """
        Standardize IIIF image URLs to ensure consistent size.
        Converts various IIIF image URLs to a standard 400px wide version.
        """
        try:
            # Skip if not a IIIF URL
            if not any(x in url.lower() for x in ["/iiif/", "info.json"]):
                return url

            # Remove any existing size parameters
            base_url = url
            for pattern in [
                "/full/full/",
                "/full/,/",
                "/full/!/",
                "/full/\d+,/",
                "/full/,\d+/",
                "/full/\d+,\d+/",
            ]:
                base_url = re.sub(pattern, "/full/", base_url, flags=re.IGNORECASE)

            # Add our standard size
            if "/full/" in base_url:
                return base_url.replace("/full/", "/full/400,/")

            return url
        except Exception as e:
            self.logger.error(f"Error standardizing IIIF URL {url}: {e}")
            return url

    def get_thumbnail_url(self) -> Optional[str]:
        """Get the appropriate thumbnail URL based on the document type."""
        try:
            doc_id = self.document.get("id")
            if not doc_id:
                return None

            # Get references
            references = self.document.get("dct_references_s")
            if isinstance(references, str):
                try:
                    references = json.loads(references)
                except json.JSONDecodeError:
                    return None

            if not isinstance(references, dict):
                return None

            thumbnail_url = None

            # Direct thumbnail URL - fastest path
            if "http://schema.org/thumbnailUrl" in references:
                url = references["http://schema.org/thumbnailUrl"]
                thumbnail_url = self._standardize_iiif_url(url)

            # IIIF Image API - fast path
            elif "http://iiif.io/api/image" in references:
                viewer_endpoint = references["http://iiif.io/api/image"]
                raw_url = f"{viewer_endpoint.replace('info.json', '')}full/400,/0/default.jpg"
                thumbnail_url = self._standardize_iiif_url(raw_url)

            # IIIF Manifest - slower path
            elif (
                "https://iiif.io/api/presentation/2/context.json" in references
                or "http://iiif.io/api/presentation#manifest" in references
            ):
                manifest_url = references.get(
                    "https://iiif.io/api/presentation/2/context.json"
                ) or references.get("http://iiif.io/api/presentation#manifest")
                raw_url = self.get_iiif_manifest_thumbnail(manifest_url)
                thumbnail_url = self._standardize_iiif_url(raw_url)

            # ESRI services
            elif "urn:x-esri:serviceType:ArcGIS#ImageMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#ImageMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#TiledMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#TiledMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#DynamicMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#DynamicMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"

            # WMS
            elif "http://www.opengis.net/def/serviceType/ogc/wms" in references:
                wms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/wms"]
                width = 200
                height = 200
                layers = self.document.get("gbl_wxsidentifier_s", "")
                thumbnail_url = (
                    f"{wms_endpoint}/reflect?"
                    f"FORMAT=image/png&"
                    f"TRANSPARENT=TRUE&"
                    f"WIDTH={width}&"
                    f"HEIGHT={height}&"
                    f"LAYERS={layers}"
                )

            # TMS
            elif "http://www.opengis.net/def/serviceType/ogc/tms" in references:
                tms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/tms"]
                thumbnail_url = (
                    f"{tms_endpoint}/reflect?format=application/vnd.google-earth.kml+xml"
                )

            if thumbnail_url:
                # Check if we have the image cached
                image_key = f"image:{hashlib.sha256(thumbnail_url.encode()).hexdigest()}"
                image_hash = hashlib.sha256(thumbnail_url.encode()).hexdigest()

                if self.image_cache.exists(image_key):
                    self.logger.info(f"🚀 Cache HIT for image {doc_id}")
                    return f"{self.application_url}/api/v1/thumbnails/{image_hash}"

                # If not cached, queue for background processing and return original URL
                self.logger.info(f"🐌 Queueing image fetch for {doc_id}: {thumbnail_url}")
                task = fetch_and_cache_image.delay(thumbnail_url)
                self.logger.info(f"Task ID: {task.id}")
                return thumbnail_url

            return None

        except Exception as e:
            self.logger.error(f"Error getting thumbnail URL: {e}")
            return None

    async def get_cached_image(self, image_hash: str) -> Optional[bytes]:
        """Retrieve a cached image by its hash."""
        try:
            image_key = f"image:{image_hash}"
            image_data = self.image_cache.get(image_key)
            if image_data:
                self.logger.debug(f"Serving cached image {image_hash}")
                return image_data
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving cached image: {e}")
            return None
