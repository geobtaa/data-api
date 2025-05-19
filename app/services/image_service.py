import hashlib
import json
import logging
import os
import re
from typing import Any, Dict, Optional

import aiohttp
import redis
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class ImageService:
    """Service for handling different types of image assets."""

    def __init__(self, metadata: Dict[str, Any]):
        """
        Initialize the image service with document metadata.

        Args:
            metadata: Document metadata dictionary
        """
        self.metadata = metadata

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

        # print(f"Document WXS: {self.metadata.get('gbl_wxsidentifier_s')}")

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
            # Check for Stanford-style thumbnail array
            if isinstance(manifest_json.get("thumbnail"), list) and manifest_json["thumbnail"]:
                self.logger.debug("Image: Stanford-style thumbnail array")
                thumbnail = manifest_json["thumbnail"][0]
                if isinstance(thumbnail, dict) and thumbnail.get("id"):
                    return thumbnail["id"]

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

    def _validate_thumbnail_url(self, url: str) -> bool:
        """
        Validate that a thumbnail URL is accessible.

        Args:
            url: The thumbnail URL to validate

        Returns:
            bool: True if URL is accessible, False otherwise
        """
        try:
            response = requests.head(url, timeout=2.0, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "").lower()
                return content_type.startswith("image/")
            self.logger.warning(f"Thumbnail URL returned status {response.status_code}: {url}")
            return False
        except Exception as e:
            self.logger.error(f"Error validating thumbnail URL {url}: {e}")
            return False

    def get_thumbnail_url(self) -> Optional[str]:
        """
        Get the thumbnail URL from document metadata with caching support.

        Returns:
            Thumbnail URL if available, None otherwise
        """
        try:
            # Check for restricted access rights
            if self.metadata.get("dct_accessrights_s") == "Restricted":
                self.logger.info("Skipping thumbnail for restricted item")
                return None

            doc_id = self.metadata.get("id")
            if not doc_id:
                return None

            # Parse references if needed
            references = self.metadata.get("dct_references_s", {})
            if isinstance(references, str):
                try:
                    references = json.loads(references)
                except json.JSONDecodeError:
                    logger.error("Failed to parse references JSON")
                    return None

            if not isinstance(references, dict):
                return None

            thumbnail_url = None

            # Check for direct thumbnail URL first
            if "http://schema.org/thumbnailUrl" in references:
                thumbnail_url = references["http://schema.org/thumbnailUrl"]
                if isinstance(thumbnail_url, list) and thumbnail_url:
                    thumbnail_url = thumbnail_url[0]

            # Check for IIIF thumbnail URL
            elif "http://iiif.io/api/image" in references:
                iiif_url = references["http://iiif.io/api/image"]
                if isinstance(iiif_url, list) and iiif_url:
                    iiif_url = iiif_url[0]
                
                # Transform ContentDM IIIF URLs
                if "contentdm.oclc.org" in iiif_url:
                    # Extract collection and item ID from the URL
                    match = re.search(r"/digital/iiif/([^/]+)/(\d+)", iiif_url)
                    if match:
                        collection, item_id = match.groups()
                        # Construct the correct IIIF URL format
                        thumbnail_url = f"https://cdm16022.contentdm.oclc.org/iiif/2/{collection}:{item_id}/full/200,/0/default.jpg"
                
                # For non-ContentDM IIIF URLs, use standard format
                if not thumbnail_url:
                    thumbnail_url = f"{iiif_url}/full/200,/0/default.jpg"

            # Check for IIIF Manifest
            elif (
                "https://iiif.io/api/presentation/2/context.json" in references
                or "http://iiif.io/api/presentation#manifest" in references
            ):
                manifest_url = references.get(
                    "https://iiif.io/api/presentation/2/context.json"
                ) or references.get("http://iiif.io/api/presentation#manifest")
                thumbnail_url = self.get_iiif_manifest_thumbnail(manifest_url)

            # Check for ESRI services
            elif "urn:x-esri:serviceType:ArcGIS#ImageMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#ImageMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#TiledMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#TiledMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            elif "urn:x-esri:serviceType:ArcGIS#DynamicMapLayer" in references:
                viewer_endpoint = references["urn:x-esri:serviceType:ArcGIS#DynamicMapLayer"]
                thumbnail_url = f"{viewer_endpoint}/info/thumbnail/thumbnail.png"
            
            # Check for WMS
            elif "http://www.opengis.net/def/serviceType/ogc/wms" in references:
                wms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/wms"]
                width = 200
                height = 200
                layers = self.metadata.get("gbl_wxsidentifier_s", "")
                thumbnail_url = (
                    f"{wms_endpoint}/reflect?"
                    f"FORMAT=image/png&"
                    f"TRANSPARENT=TRUE&"
                    f"WIDTH={width}&"
                    f"HEIGHT={height}&"
                    f"LAYERS={layers}"
                )
            
            # Check for TMS
            elif "http://www.opengis.net/def/serviceType/ogc/tms" in references:
                tms_endpoint = references["http://www.opengis.net/def/serviceType/ogc/tms"]
                thumbnail_url = (
                    f"{tms_endpoint}/reflect?format=application/vnd.google-earth.kml+xml"
                )

            if thumbnail_url:
                # Check if we have the image cached
                image_hash = hashlib.sha256(thumbnail_url.encode()).hexdigest()
                image_key = f"image:{image_hash}"
                
                if self.image_cache.exists(image_key):
                    self.logger.info(f"🚀 Cache HIT for image {doc_id}")
                    return f"{self.application_url}/api/v1/thumbnails/{image_hash}"
                
                # Validate the thumbnail URL before queueing for caching
                if not self._validate_thumbnail_url(thumbnail_url):
                    self.logger.warning(f"Invalid thumbnail URL for {doc_id}: {thumbnail_url}")
                    return None

                # If not cached, queue for background processing and return original URL
                self.logger.info(f"🐌 Queueing image fetch for {doc_id}: {thumbnail_url}")
                from app.tasks.worker import fetch_and_cache_image
                task = fetch_and_cache_image.delay(thumbnail_url)
                self.logger.info(f"Task ID: {task.id}")
                return thumbnail_url

            return None

        except Exception as e:
            logger.error(f"Error getting thumbnail URL: {str(e)}")
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

    async def get_iiif_image(self, image_url: str) -> Optional[bytes]:
        """
        Get image data from a IIIF image URL.

        Args:
            image_url: The IIIF image URL

        Returns:
            Image data in bytes, or None if retrieval fails
        """
        try:
            # Remove /info.json from URL if present
            base_url = image_url.replace("/info.json", "")

            # Get a full-size image for better OCR results
            image_url = f"{base_url}/full/full/0/default.jpg"

            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        logger.info(f"Successfully retrieved IIIF image from {image_url}")
                        return image_data
                    else:
                        logger.error(f"Failed to retrieve IIIF image: {response.status}")
                        return None

        except Exception as e:
            logger.error(f"Error retrieving IIIF image: {str(e)}")
            return None

    async def download_image(self, url: str) -> Optional[bytes]:
        """
        Download an image from a URL.

        Args:
            url: The image URL

        Returns:
            Image data in bytes, or None if download fails
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        # Check if it's an image
                        content_type = response.headers.get("content-type", "").lower()
                        if not content_type.startswith("image/"):
                            logger.warning(
                                f"URL {url} is not an image (content-type: {content_type})"
                            )
                            return None

                        image_data = await response.read()
                        logger.info(f"Successfully downloaded image from {url}")
                        return image_data
                    else:
                        logger.error(f"Failed to download image: {response.status}")
                        return None

        except Exception as e:
            logger.error(f"Error downloading image: {str(e)}")
            return None
