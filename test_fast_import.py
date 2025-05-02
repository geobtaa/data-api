import asyncio
import logging
import os
from app.gazetteer.importers.fast_importer import FastImporter

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

async def run_import():
    importer = FastImporter()
    try:
        # Check if the MARCXML file exists
        print(f"Checking MARCXML file in: {importer.data_directory}")
        
        # Run the import
        print("Starting import process...")
        result = await importer.import_data()
        print(f"Import result: {result}")
    except Exception as e:
        print(f"Error during import: {e}")

async def test_sax_import(limit=10):
    """
    Run the SAX import and stop after the specified number of records.
    
    Args:
        limit: Maximum number of records to process
    """
    importer = FastImporter()
    
    # Find the MARCXML file
    marcxml_file = os.path.join(importer.data_directory, "FASTGeographic.marcxml")
    
    if not os.path.exists(marcxml_file):
        logger.error(f"MARCXML file not found at {marcxml_file}")
        return
    
    logger.info(f"Found MARCXML file: {marcxml_file}")
    
    # Parse the MARCXML file
    logger.info(f"Starting to parse MARCXML file")
    records = importer.parse_marcxml(marcxml_file)
    
    # Limit the number of records
    if records:
        logger.info(f"Parsed {len(records)} records, limiting to {limit}")
        records = records[:limit]
        
        # Log the raw records
        for i, record in enumerate(records):
            logger.info(f"Raw Record {i+1}:")
            for key, value in record.items():
                logger.info(f"  {key}: {value}")
    else:
        logger.error("No records parsed from MARCXML file")
        return
    
    # Process the records to extract CSV data
    logger.info(f"Processing {len(records)} records for CSV format")
    processed_records = []
    
    for record in records:
        processed_record = {
            'fast_id': record.get('016_a', [None])[0],  # FAST ID from 016_a
            'uri': record.get('024_a', [None])[0],      # URI from 024_a
            'label': record.get('151_a', [None])[0],    # Label from 151_a
            'type': 'Geographic',                       # Always "Geographic" for this dataset
            'geonames': None                            # Not available in this dataset
        }
        
        # Only add records that have all required fields
        if all([processed_record['fast_id'], processed_record['uri'], processed_record['label']]):
            processed_records.append(processed_record)
    
    # Log the processed records
    logger.info(f"Processed {len(processed_records)} records for CSV format")
    for i, record in enumerate(processed_records):
        logger.info(f"CSV Record {i+1}:")
        for key, value in record.items():
            logger.info(f"  {key}: {value}")

if __name__ == '__main__':
    # Uncomment the function you want to run
    # asyncio.run(run_import())
    asyncio.run(test_sax_import(10)) 