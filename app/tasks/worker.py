from celery import Celery
import os
import requests
import logging
import redis
import hashlib

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup Celery
celery_app = Celery(
    "image_tasks",
    broker=f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', 6379)}/0",
    backend=f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', 6379)}/0",
)

# Configure Celery
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

# Setup Redis for image storage
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=1,  # Use different DB for images
    decode_responses=False,
)


@celery_app.task(bind=True, name="fetch_and_cache_image")
def fetch_and_cache_image(self, url: str) -> bool:
    """
    Fetch image from URL and store in Redis.
    Returns True if successful, False otherwise.
    """
    logger.info(f"Starting task to fetch image: {url}")
    try:
        # Generate consistent key for image
        image_key = f"image:{hashlib.sha256(url.encode()).hexdigest()}"

        # Check if already cached
        if redis_client.exists(image_key):
            logger.info(f"Image already cached: {url}")
            return True

        # Fetch image
        logger.info(f"Fetching image: {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Cache image
        ttl = int(os.getenv("REDIS_TTL", 604800))  # 7 days default
        redis_client.setex(image_key, ttl, response.content)
        logger.info(f"Successfully cached image: {url}")

        return True
    except Exception as e:
        logger.error(f"Error caching image {url}: {e}")
        self.retry(exc=e, countdown=60, max_retries=3)  # Retry failed tasks
        return False
