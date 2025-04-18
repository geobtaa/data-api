import hashlib
import inspect
import json
import logging
import os
from functools import wraps
from typing import Any, Optional

import redis.asyncio as redis

logger = logging.getLogger(__name__)

# Get Redis connection parameters from environment variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Get caching environment variable
ENDPOINT_CACHE = os.getenv("ENDPOINT_CACHE", "false").lower() == "true"

# Default cache expiration (12 hours)
DEFAULT_CACHE_TTL = int(os.getenv("CACHE_TTL", 43200))


class CacheService:
    """Service to handle Redis caching operations."""

    _instance = None
    _redis_client = None

    def __new__(cls):
        """Singleton pattern to avoid multiple Redis connections."""
        if cls._instance is None:
            cls._instance = super(CacheService, cls).__new__(cls)
            cls._instance._init_redis_client()
        return cls._instance

    def _init_redis_client(self):
        """Initialize Redis client."""
        try:
            if ENDPOINT_CACHE:
                logger.info(f"Initializing Redis client on {REDIS_HOST}:{REDIS_PORT}")
                self._redis_client = redis.Redis(
                    host=REDIS_HOST,
                    port=REDIS_PORT,
                    db=REDIS_DB,
                    password=REDIS_PASSWORD,
                    decode_responses=False,  # We'll handle serialization/deserialization ourselves
                )
                logger.info("Redis client initialized successfully")
            else:
                logger.info("Endpoint caching is disabled via ENDPOINT_CACHE environment variable")
                self._redis_client = None
        except Exception as e:
            logger.error(f"Failed to initialize Redis client: {str(e)}")
            self._redis_client = None

    async def get(self, key: str) -> Optional[Any]:
        """Get a value from cache."""
        if not self._redis_client or not ENDPOINT_CACHE:
            return None

        try:
            data = await self._redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error retrieving from cache: {str(e)}")
            return None

    async def set(self, key: str, value: Any, ttl: int = DEFAULT_CACHE_TTL) -> bool:
        """Set a value in cache with expiration."""
        if not self._redis_client or not ENDPOINT_CACHE:
            return False

        try:
            serialized = json.dumps(value)
            return await self._redis_client.set(key, serialized, ex=ttl)
        except Exception as e:
            logger.error(f"Error setting cache: {str(e)}")
            return False

    async def delete(self, key: str) -> bool:
        """Delete a value from cache."""
        if not self._redis_client or not ENDPOINT_CACHE:
            return False

        try:
            return bool(await self._redis_client.delete(key))
        except Exception as e:
            logger.error(f"Error deleting from cache: {str(e)}")
            return False

    async def flush_all(self) -> bool:
        """Flush all cache entries."""
        if not self._redis_client or not ENDPOINT_CACHE:
            return False

        try:
            return await self._redis_client.flushdb()
        except Exception as e:
            logger.error(f"Error flushing cache: {str(e)}")
            return False

    @staticmethod
    def generate_cache_key(prefix: str, *args, **kwargs) -> str:
        """Generate a deterministic cache key from arguments."""
        key_parts = [prefix]

        # Add positional args
        for arg in args:
            if isinstance(arg, (str, int, float, bool, type(None))):
                key_parts.append(str(arg))
            else:
                # For complex types, use their JSON representation
                key_parts.append(json.dumps(arg, sort_keys=True))

        # Add keyword args (sorted for consistency)
        for k in sorted(kwargs.keys()):
            v = kwargs[k]
            if isinstance(v, (str, int, float, bool, type(None))):
                key_parts.append(f"{k}={v}")
            else:
                # For complex types, use their JSON representation
                key_parts.append(f"{k}={json.dumps(v, sort_keys=True)}")

        # Join all parts and hash them
        key_string = ":".join(key_parts)
        return f"cache:{hashlib.md5(key_string.encode()).hexdigest()}"


# Create decorator for caching endpoint responses
def cached_endpoint(ttl=DEFAULT_CACHE_TTL):
    """Decorator to cache endpoint responses."""

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            if not ENDPOINT_CACHE:
                return await func(*args, **kwargs)

            # Get the function signature
            sig = inspect.signature(func)
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # Remove request object from cache key to avoid inconsistencies
            cache_args = {k: v for k, v in bound_args.arguments.items() if k != "request"}

            # Generate a cache key
            cache_key = CacheService.generate_cache_key(
                f"{func.__module__}:{func.__name__}", **cache_args
            )

            # Try to get from cache
            cache_service = CacheService()
            cached_result = await cache_service.get(cache_key)

            if cached_result is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_result

            # Cache miss, execute the function
            logger.debug(f"Cache miss for {cache_key}")
            try:
                result = await func(*args, **kwargs)
                # Only cache successful responses (status code 200)
                if isinstance(result, dict) or (
                    hasattr(result, "status_code") and result.status_code == 200
                ):
                    await cache_service.set(cache_key, result, ttl)
                return result
            except Exception as e:
                # Don't cache errors, just re-raise them
                raise

        return wrapper

    return decorator


# Create a function to invalidate cache with a prefix pattern
async def invalidate_cache_with_prefix(prefix: str) -> bool:
    """Invalidate all cache keys starting with a specific prefix."""
    if not ENDPOINT_CACHE:
        return True

    try:
        cache_service = CacheService()
        if not cache_service._redis_client:
            return False

        keys = await cache_service._redis_client.keys(f"cache:{prefix}*")
        if not keys:
            return True

        return await cache_service._redis_client.delete(*keys)
    except Exception as e:
        logger.error(f"Error invalidating cache with prefix {prefix}: {str(e)}")
        return False
