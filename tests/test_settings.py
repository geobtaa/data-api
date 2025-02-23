import os

# Test environment settings
os.environ["DATABASE_URL"] = "sqlite:///./test.db"  # Use file-based SQLite for testing
os.environ["ELASTICSEARCH_URL"] = "http://localhost:9200"
os.environ["ELASTICSEARCH_INDEX"] = "test_index"
os.environ["APPLICATION_URL"] = "http://localhost:8000"
os.environ["LOG_PATH"] = "test_logs"
os.environ["CORS_ORIGINS"] = "http://localhost:5173"

# Import and run setup
from tests.setup_db import setup_test_db
setup_test_db() 