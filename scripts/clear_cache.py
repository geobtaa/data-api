#!/usr/bin/env python
"""
Script to clear Redis cache used by the application.

This script connects to the Redis server specified in environment variables and clears
all databases. It also reports the memory usage after clearing the cache.

Environment Variables:
    REDIS_HOST: Redis server hostname (default: localhost)
    REDIS_PORT: Redis server port (default: 6379)

Usage:
    python scripts/clear_cache.py
"""

import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import redis

# Load environment variables from .env file
load_dotenv()

# Add the project root directory to Python path to allow importing app modules
sys.path.append(str(Path(__file__).parent.parent))

# Setup logging with basic configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clear_redis_cache():
    """
    Clear all Redis databases used by the application.

    This function:
    1. Connects to Redis using environment variables
    2. Clears all databases using FLUSHALL
    3. Reports success and current memory usage
    4. Handles any errors that occur during the process

    Raises:
        Exception: If there's an error connecting to Redis or clearing the cache
    """
    try:
        # Get Redis connection details from environment variables with defaults
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))

        # Connect to Redis with binary responses enabled for image data support
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=False,  # Keep binary responses for image data
        )

        # Clear all Redis databases
        redis_client.flushall()

        # Log successful cache clearing
        logger.info(f"Successfully cleared all Redis databases on {redis_host}:{redis_port}")

        # Get and log memory usage information
        memory_info = redis_client.info("memory")
        logger.info(f"Current memory usage: {memory_info.get('used_memory_human', 'unknown')}")

    except Exception as e:
        # Log and re-raise any errors that occur
        logger.error(f"Error clearing Redis cache: {e}")
        raise


if __name__ == "__main__":
    # Execute the cache clearing function when script is run directly
    clear_redis_cache()
