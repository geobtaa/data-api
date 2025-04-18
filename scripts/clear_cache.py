import logging
import os
import sys
from pathlib import Path

import redis
from dotenv import load_dotenv

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def clear_redis_cache():
    """Clear all Redis databases used by the application."""
    try:
        # Get Redis connection details from environment
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))

        # Connect to Redis
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=False,  # Keep binary responses for image data
        )

        # Clear all databases
        redis_client.flushall()

        logger.info(f"Successfully cleared all Redis databases on {redis_host}:{redis_port}")

        # Get memory info
        memory_info = redis_client.info("memory")
        logger.info(f"Current memory usage: {memory_info.get('used_memory_human', 'unknown')}")

    except Exception as e:
        logger.error(f"Error clearing Redis cache: {e}")
        raise


if __name__ == "__main__":
    clear_redis_cache()
