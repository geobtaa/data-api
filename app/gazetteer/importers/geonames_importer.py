import os
import logging
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base_importer import BaseImporter
from db.models import gazetteer_geonames

logger = logging.getLogger(__name__)

class GeonamesImporter(BaseImporter):
    """Importer for GeoNames data."""
    
    # GeoNames-specific data directory
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                           'data', 'gazetteers', 'geonames')
    
    # GeoNames field names in order
    # Based on https://download.geonames.org/export/dump/readme.txt
    FIELDNAMES = [
        'geonameid', 'name', 'asciiname', 'alternatenames', 'latitude', 'longitude',
        'feature_class', 'feature_code', 'country_code', 'cc2', 'admin1_code',
        'admin2_code', 'admin3_code', 'admin4_code', 'population', 'elevation',
        'dem', 'timezone', 'modification_date'
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get('data_directory') or self.DATA_DIR
        self.table = gazetteer_geonames
        self.table_name = 'gazetteer_geonames'
    
    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a GeoNames record before insertion.
        
        Args:
            record: The raw record from the CSV.
            
        Returns:
            Cleaned record ready for database insertion.
        """
        # Call the parent method to handle common cleaning
        record = super().clean_record(record)
        
        # Convert numeric fields
        try:
            # Integer conversions
            for int_field in ['geonameid', 'population', 'elevation', 'dem']:
                if record.get(int_field) and record[int_field] not in ['', None]:
                    record[int_field] = int(record[int_field])
                else:
                    record[int_field] = None
            
            # Decimal conversions
            for decimal_field in ['latitude', 'longitude']:
                if record.get(decimal_field) and record[decimal_field] not in ['', None]:
                    record[decimal_field] = float(record[decimal_field])
                else:
                    record[decimal_field] = None
            
            # Date conversion
            if record.get('modification_date') and record['modification_date'] not in ['', None]:
                # GeoNames date format is YYYY-MM-DD
                record['modification_date'] = datetime.strptime(record['modification_date'], '%Y-%m-%d').date()
            else:
                record['modification_date'] = None
        
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error converting field in record {record.get('geonameid')}: {e}")
        
        return record
    
    async def import_data(self) -> Dict[str, Any]:
        """
        Import GeoNames data from CSV files to the database.
        
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
            # GeoNames files are tab-delimited
            records = self.read_csv(csv_file, delimiter='\t', fieldnames=self.FIELDNAMES)
            
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
        importer = GeonamesImporter()
        result = await importer.import_data()
        print(result)
    
    asyncio.run(run_import()) 