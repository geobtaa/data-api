import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get database configuration from environment variables
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "paradedb")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "btaa_ogm_api")

# Construct database URL with asyncpg driver
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

print(f"Using database URL: {DATABASE_URL}")
