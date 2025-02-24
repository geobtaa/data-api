import os
import logging
from typing import Dict, Optional
import json
import requests
import aiohttp
import asyncio
import redis
from datetime import timedelta


class ImageService:
    def __init__(self, document: Dict):
        self.document = document
        
        # Setup Redis connection
        self.redis_host = os.getenv('REDIS_HOST', 'redis')
        self.redis_port = int(os.getenv('REDIS_PORT', 6379))
        self.cache = redis.Redis(
            host=self.redis_host, 
            port=self.redis_port, 
            db=0, 
            decode_responses=True
        )
        self.cache_ttl = int(os.getenv('REDIS_TTL', 604800))  # 7 days in seconds

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
            self.cache.setex(
                cache_key,
                self.cache_ttl,
                json.dumps(manifest_data)
            )
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

    def get_thumbnail_url(self) -> Optional[str]:
        """Get the appropriate thumbnail URL based on the document type."""
        try:
            doc_id = self.document.get('id')
            if not doc_id:
                return None

            # Check Redis cache first
            cache_key = f"thumbnail:{doc_id}"
            cached_url = self.cache.get(cache_key)
            
            if cached_url:
                self.logger.info(f"🚀 Cache HIT for thumbnail {doc_id}")
                return cached_url

            self.logger.info(f"🐌 Cache MISS for thumbnail {doc_id} - fetching...")
            
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

            # Try each method to get thumbnail URL
            if "http://schema.org/thumbnailUrl" in references:
                url = references["http://schema.org/thumbnailUrl"]
                thumbnail_url = url.replace("/full/full/0/", "/full/400,/0/") if "/full/full/0/" in url else url
                self.logger.debug(f"Found direct thumbnail URL for {doc_id}")

            elif "http://iiif.io/api/image" in references:
                viewer_endpoint = references["http://iiif.io/api/image"]
                thumbnail_url = f"{viewer_endpoint.replace('info.json', '')}full/400,/0/default.jpg"
                self.logger.debug(f"Generated IIIF image URL for {doc_id}")

            # ... rest of the thumbnail URL logic ...

            # Cache the result if we found a thumbnail
            if thumbnail_url:
                self.logger.info(f"💾 Caching new thumbnail for {doc_id}")
                self.cache.setex(cache_key, self.cache_ttl, thumbnail_url)
                
                # Log cache stats
                cache_info = {
                    'keys': len(self.cache.keys()),
                    'memory': self.cache.info()['used_memory_human'],
                    'ttl': self.cache_ttl
                }
                self.logger.debug(f"Cache stats: {cache_info}")

            return thumbnail_url

        except redis.RedisError as e:
            self.logger.error(f"Redis error for {self.document.get('id')}: {e}")
            # Continue without caching
            return self._get_thumbnail_url_without_cache()
        except Exception as e:
            self.logger.error(f"Error getting thumbnail URL for {self.document.get('id')}: {e}")
            return None

    def _get_thumbnail_url_without_cache(self) -> Optional[str]:
        """Fallback method if Redis is unavailable."""
        # Original thumbnail URL logic here
        # This is a safety fallback
        pass
