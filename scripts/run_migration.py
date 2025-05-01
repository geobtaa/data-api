#!/usr/bin/env python
"""
Script to run database migrations.

This script provides a command-line interface for running database migrations.
It supports multiple migration types and provides logging of the migration process.

Available Migrations:
    add_fast_gazetteer: Adds FAST gazetteer data to the database

Usage:
    python scripts/run_migration.py [migration_name]
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import the app modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import migration modules
from db.migrations.add_fast_gazetteer import add_fast_gazetteer

# Configure logging with standard format
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def main():
    """
    Run the specified database migration.
    
    This function:
    1. Parses command line arguments
    2. Executes the specified migration
    3. Logs the progress and results
    
    Returns:
        int: 0 on success, 1 on error
    """
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument(
        "migration",
        choices=["add_fast_gazetteer"],
        help="The migration to run",
    )
    
    # Parse command line arguments
    args = parser.parse_args()
    
    try:
        # Execute the specified migration
        if args.migration == "add_fast_gazetteer":
            logger.info("Running add_fast_gazetteer migration")
            add_fast_gazetteer()
            logger.info("Migration completed successfully")
        else:
            logger.error(f"Unknown migration: {args.migration}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.error(f"Error running migration: {e}")
        return 1


if __name__ == "__main__":
    # Run the main function and exit with appropriate status code
    sys.exit(main()) 