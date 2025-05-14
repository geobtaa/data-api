import hashlib
import logging
import os

import redis
import requests
from celery import Celery
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.getenv("LOG_PATH", "logs"), "app.log"), mode="a", encoding="utf-8"
        ),
    ],
)
logger = logging.getLogger(__name__)

# Setup Celery
celery_app = Celery(
    "tasks",
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
    worker_hijack_root_logger=False,  # Don't let Celery hijack the root logger
    worker_redirect_stdouts=False,  # Don't redirect stdout/stderr
    task_track_started=True,  # Track when tasks are started
    task_time_limit=300,  # 5 minute timeout for tasks
    task_soft_time_limit=240,  # Soft timeout 4 minutes
    worker_prefetch_multiplier=1,  # Process one task at a time
    task_acks_late=True,  # Only acknowledge tasks after they complete
    imports=[
        "app.tasks.worker",
        "app.tasks.entities",
        "app.tasks.summarization",
        "app.tasks.ocr",
    ],
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
