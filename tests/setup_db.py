import asyncio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def setup_test_db():
    """Create test database and tables."""
    # Create SQLite database
    engine = create_engine("sqlite:///:memory:")
    
    # Create tables
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS geoblacklight_development (
                id TEXT PRIMARY KEY,
                dct_title_s TEXT,
                dct_description_sm TEXT,
                dct_creator_sm TEXT,
                dct_publisher_sm TEXT,
                schema_provider_s TEXT,
                dct_references_s TEXT,
                locn_geometry TEXT,
                gbl_mdModified_dt TIMESTAMP
            )
        """))
        
        # Insert some test data
        conn.execute(text("""
            INSERT INTO geoblacklight_development (
                id, dct_title_s, dct_description_sm, dct_references_s
            ) VALUES (
                'test-123',
                'Test Document',
                'A test description',
                '{"http://schema.org/url": "http://example.com"}'
            )
        """))
        conn.commit() 