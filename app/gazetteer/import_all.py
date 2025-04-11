#!/usr/bin/env python3
"""
Import all gazetteer data.

This script runs all the gazetteer importers in sequence.
- GeoNames: Imports data from tab-delimited .txt files
- WOF: Imports data from .csv files
- BTAA: Imports data from .csv files 
"""

import asyncio
import logging
import argparse
import json
import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# Add parent directory to path to import modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.gazetteer.importers.geonames_importer import GeonamesImporter
from app.gazetteer.importers.wof_importer import WofImporter
from app.gazetteer.importers.btaa_importer import BtaaImporter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("gazetteer_import.log")],
)

logger = logging.getLogger(__name__)

# Log the database URL (without password)
db_url = os.getenv("DATABASE_URL", "")
if db_url:
    # Mask password if present
    masked_url = db_url
    if "@" in db_url and ":" in db_url:
        parts = db_url.split("@")
        auth = parts[0].split(":")
        if len(auth) > 2:  # postgresql://user:pass@host
            masked_url = f"{auth[0]}:{auth[1]}:****@{parts[1]}"
    print(f"Using database URL: {masked_url}")


async def import_all(
    gazetteer_types: List[str] = None, data_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run all gazetteer importers.

    Args:
        gazetteer_types: List of gazetteer types to import ('geonames', 'wof', 'btaa').
                        If None, all gazetteers will be imported.
        data_dir: Base directory for gazetteer data.
                If None, default directories will be used.

    Returns:
        Dictionary with import results for each gazetteer.
    """
    start_time = datetime.now()

    # Use all gazetteer types if none specified
    if not gazetteer_types:
        gazetteer_types = ["geonames", "wof", "btaa"]

    results = {}

    for gazetteer_type in gazetteer_types:
        logger.info(f"Starting import for {gazetteer_type} gazetteer")

        importer = None
        if gazetteer_type == "geonames":
            importer_dir = os.path.join(data_dir, "geonames") if data_dir else None
            importer = GeonamesImporter(data_directory=importer_dir)
            logger.info(f"GeoNames importer will look for .txt files in {importer.data_directory}")
        elif gazetteer_type == "wof":
            importer_dir = os.path.join(data_dir, "wof") if data_dir else None
            importer = WofImporter(data_directory=importer_dir)
            logger.info(
                f"WOF importer will look for specific CSV files in {importer.data_directory}:"
            )
            logger.info("  - spr.csv: Main WOF spatial records")
            logger.info("  - ancestors.csv: Ancestor relationships")
            logger.info("  - concordances.csv: Concordances to other systems")
            logger.info("  - geojson.csv: GeoJSON data (if available)")
            logger.info("  - names.csv: Alternative names")
        elif gazetteer_type == "btaa":
            importer_dir = os.path.join(data_dir, "btaa") if data_dir else None
            importer = BtaaImporter(data_directory=importer_dir)
        else:
            logger.warning(f"Unknown gazetteer type: {gazetteer_type}")
            continue

        try:
            result = await importer.import_data()
            results[gazetteer_type] = result
            logger.info(f"Finished import for {gazetteer_type} gazetteer")
        except Exception as e:
            logger.error(f"Error importing {gazetteer_type}: {e}", exc_info=True)
            results[gazetteer_type] = {"status": "error", "message": str(e), "elapsed_time": 0}

    # Calculate overall statistics
    total_records = 0
    total_errors = 0

    for gazetteer_type, result in results.items():
        if gazetteer_type == "wof" and "table_results" in result:
            # WOF has a more complex structure with tables
            for table_result in result["table_results"].values():
                total_records += table_result.get("records_processed", 0)
                total_errors += len(table_result.get("errors", []))
        else:
            total_records += result.get("records_processed", 0)
            total_errors += len(result.get("errors", []))

    elapsed_time = (datetime.now() - start_time).total_seconds()

    overall_result = {
        "status": "success" if total_errors == 0 else "partial_success",
        "gazetteers_processed": len(results),
        "total_records_processed": total_records,
        "total_errors": total_errors,
        "elapsed_time": elapsed_time,
        "records_per_second": total_records / elapsed_time if elapsed_time > 0 else 0,
        "gazetteer_results": results,
    }

    logger.info(
        f"All imports completed. {total_records} records processed across {len(results)} gazetteers in {elapsed_time:.2f} seconds"
    )

    return overall_result


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Import gazetteer data")

    parser.add_argument(
        "--gazetteers",
        nargs="+",
        choices=["geonames", "wof", "btaa", "all"],
        default=["all"],
        help="Gazetteers to import (default: all)",
    )

    parser.add_argument("--data-dir", type=str, help="Base directory for gazetteer data")

    parser.add_argument("--output", type=str, help="Output file for import results (JSON format)")

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Convert 'all' to all gazetteer types
    gazetteer_types = []
    if "all" in args.gazetteers:
        gazetteer_types = ["geonames", "wof", "btaa"]
    else:
        gazetteer_types = args.gazetteers

    async def run():
        logger.info(f"Starting import of {', '.join(gazetteer_types)} gazetteers")
        result = await import_all(gazetteer_types, args.data_dir)

        # Print summary
        print("\nImport Summary:")
        print(f"Status: {result['status']}")
        print(f"Gazetteers processed: {result['gazetteers_processed']}")
        print(f"Total records: {result['total_records_processed']}")
        print(f"Total errors: {result['total_errors']}")
        print(f"Total time: {result['elapsed_time']:.2f} seconds")
        print(f"Records per second: {result['records_per_second']:.2f}")

        # Write results to file if requested
        if args.output:
            with open(args.output, "w") as f:
                json.dump(result, f, indent=2)
            print(f"\nDetailed results written to {args.output}")

        return result

    asyncio.run(run())
