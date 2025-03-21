import os
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base_importer import BaseImporter
from db.models import gazetteer_btaa

logger = logging.getLogger(__name__)

class BtaaImporter(BaseImporter):
    """Importer for BTAA gazetteer data."""
    
    # BTAA-specific data directory
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                           'data', 'gazetteers', 'btaa')
    
    # BTAA fieldnames
    FIELDNAMES = [
        'fast_area', 'bounding_box', 'geometry', 'geonames_id', 'state_abbv',
        'state_name', 'county_fips', 'statefp', 'namelsad'
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get('data_directory') or self.DATA_DIR
        self.table = gazetteer_btaa
        self.table_name = 'gazetteer_btaa'
    
    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a BTAA record before insertion.
        
        Args:
            record: The raw record from the CSV.
            
        Returns:
            Cleaned record ready for database insertion.
        """
        # Call the parent method to handle common cleaning
        record = super().clean_record(record)
        
        # Ensure state_abbv is uppercase
        if record.get('state_abbv'):
            record['state_abbv'] = record['state_abbv'].upper()
            
            # Validate state_abbv (2 character limit)
            if len(record['state_abbv']) > 2:
                record['state_abbv'] = record['state_abbv'][:2]
        
        return record
    
    async def import_data(self) -> Dict[str, Any]:
        """
        Import BTAA data from CSV files to the database.
        
        Returns:
            Dictionary with import statistics.
        """
        start_time = datetime.now()
        self.find_csv_files()
        
        if not self.csv_files:
            self.logger.error(f"No CSV files found in {self.data_directory}")
            return {
                "status": "error",
                "message": f"No CSV files found in {self.data_directory}",
                "elapsed_time": (datetime.now() - start_time).total_seconds()
            }
        
        # Truncate the table if it exists
        await self.truncate_table(self.table_name)
        
        total_processed = 0
        
        for csv_file in self.csv_files:
            self.logger.info(f"Processing file: {csv_file}")
            
            # Read and process the CSV file
            records = self.read_csv(csv_file, delimiter=',')
            
            if not records:
                self.logger.warning(f"No records found in {csv_file}")
                continue
            
            self.logger.info(f"Found {len(records)} records in {csv_file}")
            
            # Clean the records
            cleaned_records = [self.clean_record(record) for record in records]
            
            # Bulk insert the records
            inserted = await self.bulk_insert(self.table, cleaned_records)
            
            self.logger.info(f"Inserted {inserted} records from {csv_file}")
            total_processed += inserted
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            "status": "success" if not self.errors else "partial_success",
            "files_processed": len(self.csv_files),
            "records_processed": total_processed,
            "errors": self.errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_processed / elapsed_time if elapsed_time > 0 else 0
        }
        
        self.logger.info(f"Import completed. {total_processed} records processed in {elapsed_time:.2f} seconds")
        
        return result

# Run this module directly to test the importer
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def run_import():
        importer = BtaaImporter()
        result = await importer.import_data()
        print(result)
    
    asyncio.run(run_import()) 