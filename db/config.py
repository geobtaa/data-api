import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the database URL with a default value
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@paradedb:5432/btaa_geometadata_api"
)

# Convert the DATABASE_URL to use asyncpg if it's using postgres://
if DATABASE_URL.startswith("postgresql://"):
    ASYNC_DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    ASYNC_DATABASE_URL = DATABASE_URL

print(f"Using database URL: {DATABASE_URL}")  # Debug line
