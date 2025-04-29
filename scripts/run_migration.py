#!/usr/bin/env python
"""
Script to run database migrations.
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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

logger = logging.getLogger(__name__)


def main():
    """Run the specified migration."""
    parser = argparse.ArgumentParser(description="Run database migrations")
    parser.add_argument(
        "migration",
        choices=["add_fast_gazetteer"],
        help="The migration to run",
    )
    
    args = parser.parse_args()
    
    if args.migration == "add_fast_gazetteer":
        logger.info("Running add_fast_gazetteer migration")
        add_fast_gazetteer()
        logger.info("Migration completed successfully")
    else:
        logger.error(f"Unknown migration: {args.migration}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main()) 