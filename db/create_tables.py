import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

from db.models import metadata

# Load environment variables
load_dotenv()

# Get the database URL
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:postgres@paradedb:5432/geoblacklight_development"
)


def create_tables():
    """Create all tables defined in models.py"""
    engine = create_engine(DATABASE_URL)
    metadata.create_all(engine)
    print("All tables created successfully!")


if __name__ == "__main__":
    create_tables()
