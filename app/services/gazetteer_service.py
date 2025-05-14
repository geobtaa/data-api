import logging
import os
from typing import Any, Dict, Optional

import asyncpg
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)


class GazetteerService:
    """
    Service for interacting with the local GeoNames database.
    Provides methods for looking up geographic places and entities.
    """

    def __init__(self, db_connection=None):
        """
        Initialize the GazetteerService.

        Args:
            db_connection: Optional existing database connection to use.
                          If not provided, a new connection will be created.
        """
        self.db_connection = db_connection
        self.db_pool = None
        logger.info("Initialized GazetteerService")

    async def connect(self):
        """
        Connect to the database if not already connected.
        """
        if self.db_connection is None and self.db_pool is None:
            # Get database URL from environment variables
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL environment variable is not set")

            try:
                # Create a connection pool using the DATABASE_URL
                self.db_pool = await asyncpg.create_pool(database_url)
                logger.info(f"Connected to database at {database_url}")
            except Exception as e:
                logger.error(f"Failed to connect to database: {str(e)}")
                raise

    async def disconnect(self):
        """
        Disconnect from the database.
        """
        if self.db_pool:
            await self.db_pool.close()
            self.db_pool = None
            logger.info("Disconnected from database")

    async def lookup_place(
        self, name: str, entity_type: Optional[str] = None, context: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Look up a place in the gazetteer database.

        Args:
            name: The name of the place to look up
            entity_type: Optional type of the geographic entity (e.g., city, river)
            context: Optional additional context to help with disambiguation

        Returns:
            Dictionary containing place information or None if not found
        """
        await self.connect()

        try:
            # Determine the feature class based on entity type
            feature_class = self._get_feature_class(entity_type) if entity_type else None

            # Build the query based on available parameters
            query = """
                SELECT 
                    geonameid as id,
                    name,
                    asciiname,
                    alternatenames,
                    feature_class,
                    feature_code,
                    country_code,
                    cc2,
                    admin1_code,
                    admin2_code,
                    admin3_code,
                    admin4_code,
                    population,
                    elevation,
                    dem,
                    timezone,
                    modification_date,
                    latitude,
                    longitude
                FROM gazetteer_geonames
                WHERE name ILIKE $1
            """
            params = [f"%{name}%"]
            param_index = 2

            if feature_class:
                query += f" AND feature_class = ${param_index}"
                params.append(feature_class)
                param_index += 1

            # Order by population (if available) to prioritize more significant places
            query += " ORDER BY population DESC NULLS LAST LIMIT 1"

            # Log the query
            logger.info(f"Executing query: {query}")
            logger.info(f"Parameters: {params}")

            # Execute the query
            conn = await self.db_pool.acquire()
            try:
                row = await conn.fetchrow(query, *params)
                if row:
                    # Convert the row to a dictionary
                    result = dict(row)

                    # Add a confidence score based on name match
                    result["confidence"] = self._calculate_confidence(result, name, entity_type)

                    # Add country name if country code is available
                    if result.get("country_code"):
                        country_name = await self._get_country_name(result["country_code"])
                        if country_name:
                            result["country"] = country_name

                    # Add entity type based on feature class and code
                    result["type"] = self._get_entity_type(
                        result.get("feature_class"), result.get("feature_code")
                    )

                    logger.info(f"Found place: {result['name']} ({result['id']})")
                    return result
                else:
                    logger.warning(f"No place found for name: {name}")
                    return None
            finally:
                await self.db_pool.release(conn)

        except Exception as e:
            logger.error(f"Error looking up place '{name}': {str(e)}")
            return None

    async def _get_country_name(self, country_code: str) -> Optional[str]:
        """
        Get the full country name from the country code.

        Args:
            country_code: ISO country code

        Returns:
            Country name or None if not found
        """
        try:
            conn = await self.db_pool.acquire()
            try:
                row = await conn.fetchrow(
                    "SELECT name FROM countryinfo WHERE iso_alpha2 = $1", country_code
                )
                return row["name"] if row else None
            finally:
                await self.db_pool.release(conn)
        except Exception as e:
            logger.error(f"Error getting country name for code '{country_code}': {str(e)}")
            return None

    def _get_feature_class(self, entity_type: str) -> Optional[str]:
        """
        Map entity types to GeoNames feature classes.

        Args:
            entity_type: Type of the geographic entity

        Returns:
            GeoNames feature class code or None if not mapped
        """
        # Map common entity types to GeoNames feature classes
        # See: https://www.geonames.org/export/codes.html
        feature_class_map = {
            "country": "A",
            "state": "A",
            "province": "A",
            "city": "P",
            "town": "P",
            "village": "P",
            "river": "H",
            "lake": "H",
            "ocean": "H",
            "sea": "H",
            "mountain": "T",
            "hill": "T",
            "valley": "T",
            "forest": "V",
            "park": "L",
            "island": "L",
            "peninsula": "L",
        }

        return feature_class_map.get(entity_type.lower())

    def _get_entity_type(self, feature_class: Optional[str], feature_code: Optional[str]) -> str:
        """
        Convert GeoNames feature class and code to a human-readable entity type.

        Args:
            feature_class: GeoNames feature class
            feature_code: GeoNames feature code

        Returns:
            Human-readable entity type
        """
        if not feature_class:
            return "unknown"

        # Map feature classes to entity types
        class_map = {
            "A": "administrative",
            "H": "hydrographic",
            "L": "area",
            "P": "populated place",
            "R": "road",
            "S": "spot",
            "T": "hypsographic",
            "U": "undersea",
            "V": "vegetation",
        }

        # Get the base type from the feature class
        entity_type = class_map.get(feature_class, "unknown")

        # Refine the type based on the feature code if available
        if feature_code:
            # This is a simplified approach - in a real implementation,
            # you might want to use a more comprehensive mapping
            if feature_class == "P":
                if feature_code == "PPL":
                    entity_type = "city"
                elif feature_code == "PPLA":
                    entity_type = "capital"
                elif feature_code == "PPLG":
                    entity_type = "seat of government"
            elif feature_class == "H":
                if feature_code == "STM":
                    entity_type = "stream"
                elif feature_code == "RV":
                    entity_type = "river"
                elif feature_code == "LK":
                    entity_type = "lake"
                elif feature_code == "OCN":
                    entity_type = "ocean"
                elif feature_code == "SEA":
                    entity_type = "sea"

        return entity_type

    def _calculate_confidence(
        self, result: Dict[str, Any], query_name: str, entity_type: Optional[str]
    ) -> float:
        """
        Calculate a confidence score for the match.

        Args:
            result: The database result
            query_name: The name used in the query
            entity_type: The type of entity being searched for

        Returns:
            Confidence score between 0 and 1
        """
        confidence = 0.5  # Base confidence

        # Exact name match
        if result["name"].lower() == query_name.lower():
            confidence += 0.3
        elif result["asciiname"].lower() == query_name.lower():
            confidence += 0.2

        # Entity type match
        if (
            entity_type
            and self._get_entity_type(result.get("feature_class"), result.get("feature_code"))
            == entity_type.lower()
        ):
            confidence += 0.2

        # Population factor (more populous places are more likely to be the intended match)
        if result.get("population"):
            # Normalize population to a 0-0.1 range (log scale)
            population_factor = min(0.1, max(0, (result["population"] / 10000000) * 0.1))
            confidence += population_factor

        return min(1.0, confidence)
