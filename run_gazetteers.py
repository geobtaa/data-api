#!/usr/bin/env python
"""
Script to download and import gazetteer data.

This script:
1. Downloads data from all supported gazetteers (GeoNames, Who's on First, BTAA, FAST)
2. Imports the downloaded data into the database
3. Provides detailed logging of the process

Environment Variables:
    LOG_PATH: Optional path for log files (default: logs)
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Add the project root directory to Python path
sys.path.append(str(Path(__file__).parent))

# Load environment variables
load_dotenv()

# Setup logging
log_path = os.getenv("LOG_PATH", "logs")
os.makedirs(log_path, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Print to console
        logging.FileHandler(f"{log_path}/gazetteers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),  # Write to file
    ],
)
logger = logging.getLogger(__name__)

# Import the gazetteer modules
from app.gazetteer.download import download_gazetteer
from app.gazetteer.import_all import import_all

def check_gazetteer_data_exists(gazetteer: str) -> bool:
    """
    Check if data already exists for a given gazetteer.
    
    Args:
        gazetteer: Name of the gazetteer to check
        
    Returns:
        bool: True if data exists, False otherwise
    """
    data_dir = Path("data/gazetteers")
    
    if gazetteer == "geonames":
        # Check for US.txt file
        return (data_dir / "geonames" / "US.txt").exists()
    elif gazetteer == "wof":
        # Check for both the SQLite DB and CSV files
        wof_dir = data_dir / "wof"
        csv_dir = wof_dir / "csv"
        return (
            (wof_dir / "whosonfirst-data-admin-us-latest.db").exists() and
            csv_dir.exists() and
            any(csv_dir.glob("*.csv"))
        )
    elif gazetteer == "fast":
        # Check for MARCXML file
        return (data_dir / "fast" / "FASTGeographic.marcxml").exists()
    elif gazetteer == "btaa":
        # BTAA data is not downloaded, it's created from other sources
        return True
    
    return False

async def run_gazetteers():
    """Run the complete gazetteer download and import process."""
    start_time = datetime.now()
    logger.info("Starting gazetteer download and import process...")

    try:
        # Step 1: Download gazetteer data
        logger.info("Step 1: Downloading gazetteer data...")
        download_results = {}
        for gazetteer in ["geonames", "wof", "btaa", "fast"]:
            if check_gazetteer_data_exists(gazetteer):
                logger.info(f"Data already exists for {gazetteer}, skipping download")
                download_results[gazetteer] = {
                    "status": "success",
                    "message": "Data already exists, skipped download"
                }
                continue
                
            logger.info(f"Downloading {gazetteer} data...")
            result = download_gazetteer(gazetteer, download=True, export=True, all_ops=True)
            download_results[gazetteer] = result
            if result.get("status") == "error":
                logger.error(f"Error downloading {gazetteer}: {result.get('error')}")
            else:
                logger.info(f"Successfully downloaded {gazetteer} data")

        # Step 2: Import gazetteer data
        logger.info("Step 2: Importing gazetteer data...")
        import_results = await import_all()
        
        # Log results
        logger.info("\nDownload Results:")
        for gazetteer, result in download_results.items():
            status = "Success" if result.get("status") != "error" else "Failed"
            logger.info(f"{gazetteer}: {status}")
            if result.get("status") == "error":
                logger.error(f"Error: {result.get('error')}")
            elif result.get("message"):
                logger.info(f"Note: {result.get('message')}")

        logger.info("\nImport Results:")
        logger.info(f"Overall Status: {import_results['status']}")
        logger.info(f"Total Records Processed: {import_results['total_records_processed']}")
        logger.info(f"Total Errors: {import_results['total_errors']}")
        logger.info(f"Total Time: {import_results['elapsed_time']:.2f} seconds")
        logger.info(f"Records per second: {import_results['records_per_second']:.2f}")
        
        logger.info("\nGazetteer-specific Results:")
        for gazetteer, result in import_results['gazetteer_results'].items():
            status = "Success" if result.get("status") != "error" else "Failed"
            logger.info(f"{gazetteer}: {status}")
            if result.get("status") == "error":
                logger.error(f"Error: {result.get('message')}")
            else:
                logger.info(f"Records imported: {result.get('records_processed', 0)}")

        # Calculate and log total time
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        logger.info(f"\nProcess completed in {total_time:.2f} seconds")

    except Exception as e:
        logger.error(f"Error in gazetteer process: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(run_gazetteers()) 