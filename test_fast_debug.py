#!/usr/bin/env python3
import asyncio
import logging
import os
from app.gazetteer.importers.fast_importer import FastImporter

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for more detailed output
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("fast_debug")

async def debug_fast_import():
    try:
        importer = FastImporter()
        
        # Step 1: Check if MARCXML file exists
        marcxml_path = os.path.join(importer.data_directory, "FASTGeographic.marcxml")
        if os.path.exists(marcxml_path):
            logger.info(f"Found MARCXML file: {marcxml_path}")
            logger.info(f"File size: {os.path.getsize(marcxml_path)} bytes")
        else:
            logger.error(f"MARCXML file not found at: {marcxml_path}")
            return
        
        # Step 2: Parse MARCXML
        logger.info("Starting MARCXML parsing...")
        records = importer.parse_marcxml(marcxml_path)
        logger.info(f"Parsed {len(records)} records")
        
        # Log sample of first few records
        if records:
            logger.info("Sample of first 2 records:")
            for i, record in enumerate(records[:2]):
                logger.info(f"Record {i + 1}:")
                for key, value in record.items():
                    logger.info(f"  {key}: {value}")
        
        # Step 3: Process records
        logger.info("Processing records...")
        processed_records = importer.process_records(records)
        logger.info(f"Processed {len(processed_records)} records")
        
        # Log sample of processed records
        if processed_records:
            logger.info("Sample of first 2 processed records:")
            for i, record in enumerate(processed_records[:2]):
                logger.info(f"Record {i + 1}:")
                for key, value in record.items():
                    logger.info(f"  {key}: {value}")
        
        # Step 4: Export to CSV
        logger.info("Exporting to CSV...")
        csv_path = importer.export_to_csv(processed_records)
        logger.info(f"Exported to: {csv_path}")
        
        if os.path.exists(csv_path):
            logger.info(f"CSV file size: {os.path.getsize(csv_path)} bytes")
            # Read and display first few lines of CSV
            with open(csv_path, 'r', encoding='utf-8') as f:
                logger.info("First few lines of CSV:")
                for i, line in enumerate(f):
                    if i < 3:  # Show header + 2 data lines
                        logger.info(line.strip())
                    else:
                        break
        
    except Exception as e:
        logger.error(f"Error during debug: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(debug_fast_import()) 