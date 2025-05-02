import logging
import sys
from pathlib import Path

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    Text,
    create_engine,
    inspect,
)
from sqlalchemy.sql import text

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from db.config import DATABASE_URL

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_gazetteer_tables():
    """Create the gazetteer tables."""
    try:
        # Create engine and inspector
        engine = create_engine(DATABASE_URL)
        inspector = inspect(engine)

        # Create MetaData instance
        metadata = MetaData()

        # Define the gazetteer_geonames table
        gazetteer_geonames = Table(
            "gazetteer_geonames",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("geonameid", BigInteger, nullable=False, unique=True, index=True),
            Column("name", String, nullable=False),
            Column("asciiname", String, nullable=False),
            Column("alternatenames", Text),
            Column("latitude", Numeric(10, 7), nullable=False),
            Column("longitude", Numeric(10, 7), nullable=False),
            Column("feature_class", String(1)),
            Column("feature_code", String(10)),
            Column("country_code", String(2)),
            Column("cc2", String(200)),
            Column("admin1_code", String(20)),
            Column("admin2_code", String(80)),
            Column("admin3_code", String(20)),
            Column("admin4_code", String(20)),
            Column("population", BigInteger),
            Column("elevation", Integer),
            Column("dem", Integer),
            Column("timezone", String(40)),
            Column("modification_date", Date),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_wof_spr table (spatial pyramid resolution)
        gazetteer_wof_spr = Table(
            "gazetteer_wof_spr",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("wok_id", BigInteger, nullable=False, unique=True, index=True),
            Column("parent_id", BigInteger),
            Column("name", String, nullable=False),
            Column("placetype", String),
            Column("country", String(2)),
            Column("repo", String),
            Column("latitude", Numeric(10, 7)),
            Column("longitude", Numeric(10, 7)),
            Column("min_latitude", Numeric(10, 7)),
            Column("min_longitude", Numeric(10, 7)),
            Column("max_latitude", Numeric(10, 7)),
            Column("max_longitude", Numeric(10, 7)),
            Column("is_current", Integer),
            Column("is_deprecated", Integer),
            Column("is_ceased", Integer),
            Column("is_superseded", Integer),
            Column("is_superseding", Integer),
            Column("superseded_by", Integer),
            Column("supersedes", Integer),
            Column("lastmodified", Integer),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_wof_ancestors table
        gazetteer_wof_ancestors = Table(
            "gazetteer_wof_ancestors",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("wok_id", BigInteger, nullable=False, index=True),
            Column("ancestor_id", Integer, nullable=False),
            Column("ancestor_placetype", String),
            Column("lastmodified", Integer),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_wof_concordances table
        gazetteer_wof_concordances = Table(
            "gazetteer_wof_concordances",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("wok_id", BigInteger, nullable=False, index=True),
            Column("other_id", String, nullable=False),
            Column("other_source", String, nullable=False),
            Column("lastmodified", Integer),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_wof_geojson table
        gazetteer_wof_geojson = Table(
            "gazetteer_wof_geojson",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("wok_id", BigInteger, nullable=False, index=True),
            Column("body", Text, nullable=False),
            Column("source", String),
            Column("alt_label", String),
            Column("is_alt", Boolean),
            Column("lastmodified", Integer),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_wof_names table
        gazetteer_wof_names = Table(
            "gazetteer_wof_names",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("wok_id", BigInteger, nullable=False, index=True),
            Column("placetype", String),
            Column("country", String(2)),
            Column("language", String),
            Column("extlang", String),
            Column("script", String),
            Column("region", String),
            Column("variant", String),
            Column("extension", String),
            Column("privateuse", String),
            Column("name", String, nullable=False),
            Column("lastmodified", Integer),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Define the gazetteer_btaa table
        gazetteer_btaa = Table(
            "gazetteer_btaa",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("fast_area", String, nullable=False, index=True),
            Column("bounding_box", String),
            Column("geometry", Text),
            Column("geonames_id", String),
            Column("state_abbv", String(2), index=True),
            Column("state_name", String),
            Column("county_fips", String, index=True),
            Column("statefp", String),
            Column("namelsad", String),
            Column("created_at", Date),
            Column("updated_at", Date),
        )

        # Create tables if they don't exist
        tables_to_create = [
            ("gazetteer_geonames", gazetteer_geonames),
            ("gazetteer_wof_spr", gazetteer_wof_spr),
            ("gazetteer_wof_ancestors", gazetteer_wof_ancestors),
            ("gazetteer_wof_concordances", gazetteer_wof_concordances),
            ("gazetteer_wof_geojson", gazetteer_wof_geojson),
            ("gazetteer_wof_names", gazetteer_wof_names),
            ("gazetteer_btaa", gazetteer_btaa),
        ]

        # Create tables and indexes
        for table_name, table in tables_to_create:
            if not inspector.has_table(table_name):
                table.create(engine)
                logger.info(f"Created {table_name} table")
            else:
                logger.info(f"{table_name} table already exists")

        # Create additional indexes
        with engine.connect() as conn:
            # Add compound index for state_abbv and namelsad on gazetteer_btaa
            if not inspector.has_index("gazetteer_btaa", "idx_state_abbv_namelsad"):
                conn.execute(
                    text(
                        """
                    CREATE INDEX idx_state_abbv_namelsad ON gazetteer_btaa(state_abbv, namelsad);
                """
                    )
                )
                logger.info("Created compound index on gazetteer_btaa(state_abbv, namelsad)")

            # Add additional indexes for optimized querying
            conn.execute(
                text(
                    """
                CREATE INDEX IF NOT EXISTS idx_geonames_name ON gazetteer_geonames(name);
                CREATE INDEX IF NOT EXISTS idx_geonames_country_feature ON gazetteer_geonames(country_code, feature_class, feature_code);
                CREATE INDEX IF NOT EXISTS idx_geonames_admin ON gazetteer_geonames(country_code, admin1_code, admin2_code);
                
                CREATE INDEX IF NOT EXISTS idx_wof_spr_name ON gazetteer_wof_spr(name);
                CREATE INDEX IF NOT EXISTS idx_wof_spr_placetype ON gazetteer_wof_spr(placetype);
                CREATE INDEX IF NOT EXISTS idx_wof_spr_country ON gazetteer_wof_spr(country);
                
                CREATE INDEX IF NOT EXISTS idx_wof_names_name ON gazetteer_wof_names(name);
                CREATE INDEX IF NOT EXISTS idx_wof_names_country ON gazetteer_wof_names(country);
            """
                )
            )
            conn.commit()
            logger.info("Created additional indexes for all gazetteer tables")

    except Exception as e:
        logger.error(f"Error creating gazetteer tables: {e}")
        raise


if __name__ == "__main__":
    create_gazetteer_tables()
