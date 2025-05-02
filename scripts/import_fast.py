#!/usr/bin/env python
"""
Script to import the OCLC FAST Dataset Geographic entries.

This script imports geographic entries from the OCLC FAST Dataset into the database.
It uses an asynchronous importer to handle the data efficiently and provides
detailed progress reporting and error handling.

Usage:
    python scripts/import_fast.py
"""

import asyncio
import logging
import os
import sys

# Add the parent directory to the path so we can import the app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.gazetteer.importers import FastImporter

# Configure logging with standard format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


async def main():
    """
    Run the FAST dataset import process.

    This function:
    1. Initializes the FAST importer
    2. Executes the import process
    3. Logs detailed results including:
       - Import status
       - Number of records processed
       - Number of records inserted
       - Elapsed time
       - Any errors encountered

    Returns:
        bool: True if import was successful, False otherwise
    """
    logger.info("Starting FAST importer")

    try:
        # Initialize and run the importer
        importer = FastImporter()
        result = await importer.import_data()

        # Log import results
        logger.info(f"Import completed with status: {result['status']}")
        logger.info(f"Records processed: {result.get('records_processed', 0)}")
        logger.info(f"Records inserted: {result.get('records_inserted', 0)}")
        logger.info(f"Elapsed time: {result.get('elapsed_time', 0):.2f} seconds")

        # Log any errors that occurred
        if result.get("errors"):
            logger.error(f"Errors encountered: {result['errors']}")

        return result["status"] == "success"

    except Exception as e:
        logger.error(f"Error during FAST import: {e}")
        return False


if __name__ == "__main__":
    # Run the import process and exit with appropriate status code
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
