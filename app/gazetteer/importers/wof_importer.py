import os
import logging
import asyncio
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

from .base_importer import BaseImporter
from db.models import (
    gazetteer_wof_spr, 
    gazetteer_wof_ancestors, 
    gazetteer_wof_concordances,
    gazetteer_wof_geojson,
    gazetteer_wof_names
)

logger = logging.getLogger(__name__)

class WofImporter(BaseImporter):
    """Importer for Who's on First (WOF) data."""
    
    # WOF-specific data directory
    DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), 
                           'data', 'gazetteers', 'wof')
    
    # WOF field mappings for each table
    SPR_FIELDNAMES = [
        'wok_id', 'parent_id', 'name', 'placetype', 'country', 'repo',
        'latitude', 'longitude', 'min_latitude', 'min_longitude', 'max_latitude', 'max_longitude',
        'is_current', 'is_deprecated', 'is_ceased', 'is_superseded', 'is_superseding',
        'superseded_by', 'supersedes', 'lastmodified'
    ]
    
    ANCESTORS_FIELDNAMES = [
        'wok_id', 'ancestor_id', 'ancestor_placetype', 'lastmodified'
    ]
    
    CONCORDANCES_FIELDNAMES = [
        'wok_id', 'other_id', 'other_source', 'lastmodified'
    ]
    
    GEOJSON_FIELDNAMES = [
        'wok_id', 'body', 'source', 'alt_label', 'is_alt', 'lastmodified'
    ]
    
    NAMES_FIELDNAMES = [
        'wok_id', 'placetype', 'country', 'language', 'extlang', 'script',
        'region', 'variant', 'extension', 'privateuse', 'name', 'lastmodified'
    ]
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get('data_directory') or self.DATA_DIR
        
        # Dictionary of tables
        self.tables = {
            'spr': {
                'table': gazetteer_wof_spr,
                'name': 'gazetteer_wof_spr',
                'fieldnames': self.SPR_FIELDNAMES
            },
            'ancestors': {
                'table': gazetteer_wof_ancestors,
                'name': 'gazetteer_wof_ancestors',
                'fieldnames': self.ANCESTORS_FIELDNAMES
            },
            'concordances': {
                'table': gazetteer_wof_concordances,
                'name': 'gazetteer_wof_concordances',
                'fieldnames': self.CONCORDANCES_FIELDNAMES
            },
            'geojson': {
                'table': gazetteer_wof_geojson,
                'name': 'gazetteer_wof_geojson',
                'fieldnames': self.GEOJSON_FIELDNAMES
            },
            'names': {
                'table': gazetteer_wof_names,
                'name': 'gazetteer_wof_names',
                'fieldnames': self.NAMES_FIELDNAMES
            }
        }
    
    def clean_record(self, record: Dict[str, Any], table_type: str) -> Dict[str, Any]:
        """
        Clean and transform a WOF record before insertion.
        
        Args:
            record: The raw record from the CSV.
            table_type: Type of table (spr, ancestors, concordances, geojson, names).
            
        Returns:
            Cleaned record ready for database insertion.
        """
        # Call the parent method to handle common cleaning
        record = super().clean_record(record)
        
        try:
            # Handle specific conversions based on table type
            if table_type == 'spr':
                # BigInteger conversions
                for big_int_field in ['wok_id', 'parent_id']:
                    if record.get(big_int_field) and record[big_int_field] not in ['', None]:
                        record[big_int_field] = int(record[big_int_field])
                    else:
                        record[big_int_field] = None
                
                # Integer conversions
                for int_field in ['is_current', 'is_deprecated', 'is_ceased', 'is_superseded', 
                                  'is_superseding', 'superseded_by', 'supersedes', 'lastmodified']:
                    if record.get(int_field) and record[int_field] not in ['', None]:
                        record[int_field] = int(record[int_field])
                    else:
                        record[int_field] = None
                
                # Decimal conversions
                for decimal_field in ['latitude', 'longitude', 'min_latitude', 'min_longitude', 
                                      'max_latitude', 'max_longitude']:
                    if record.get(decimal_field) and record[decimal_field] not in ['', None]:
                        record[decimal_field] = float(record[decimal_field])
                    else:
                        record[decimal_field] = None
            
            elif table_type == 'ancestors':
                # BigInteger and Integer conversions
                if record.get('wok_id') and record['wok_id'] not in ['', None]:
                    record['wok_id'] = int(record['wok_id'])
                else:
                    record['wok_id'] = None
                
                if record.get('ancestor_id') and record['ancestor_id'] not in ['', None]:
                    record['ancestor_id'] = int(record['ancestor_id'])
                else:
                    record['ancestor_id'] = None
                
                if record.get('lastmodified') and record['lastmodified'] not in ['', None]:
                    record['lastmodified'] = int(record['lastmodified'])
                else:
                    record['lastmodified'] = None
            
            elif table_type == 'concordances':
                # BigInteger conversion
                if record.get('wok_id') and record['wok_id'] not in ['', None]:
                    record['wok_id'] = int(record['wok_id'])
                else:
                    record['wok_id'] = None
                
                if record.get('lastmodified') and record['lastmodified'] not in ['', None]:
                    record['lastmodified'] = int(record['lastmodified'])
                else:
                    record['lastmodified'] = None
            
            elif table_type == 'geojson':
                # BigInteger conversion
                if record.get('wok_id') and record['wok_id'] not in ['', None]:
                    record['wok_id'] = int(record['wok_id'])
                else:
                    record['wok_id'] = None
                
                # Boolean conversion
                if record.get('is_alt') is not None:
                    if isinstance(record['is_alt'], str):
                        record['is_alt'] = record['is_alt'].lower() in ['true', '1', 't', 'yes', 'y']
                
                if record.get('lastmodified') and record['lastmodified'] not in ['', None]:
                    record['lastmodified'] = int(record['lastmodified'])
                else:
                    record['lastmodified'] = None
            
            elif table_type == 'names':
                # BigInteger conversion
                if record.get('wok_id') and record['wok_id'] not in ['', None]:
                    record['wok_id'] = int(record['wok_id'])
                else:
                    record['wok_id'] = None
                
                if record.get('lastmodified') and record['lastmodified'] not in ['', None]:
                    record['lastmodified'] = int(record['lastmodified'])
                else:
                    record['lastmodified'] = None
        
        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error converting field in {table_type} record {record.get('wok_id')}: {e}")
        
        return record
    
    async def import_table_data(self, table_type: str) -> Dict[str, Any]:
        """
        Import data for a specific WOF table.
        
        Args:
            table_type: Type of table (spr, ancestors, concordances, geojson, names).
            
        Returns:
            Dictionary with import statistics.
        """
        start_time = datetime.now()
        
        # Get table information
        table_info = self.tables.get(table_type)
        if not table_info:
            self.logger.error(f"Unknown table type: {table_type}")
            return {
                "status": "error",
                "message": f"Unknown table type: {table_type}",
                "elapsed_time": 0
            }
        
        table = table_info['table']
        table_name = table_info['name']
        fieldnames = table_info['fieldnames']
        
        # Define the directory for this table type
        table_dir = os.path.join(self.data_directory, table_type)
        
        if not os.path.exists(table_dir):
            self.logger.warning(f"Directory not found for {table_type}: {table_dir}")
            return {
                "status": "warning",
                "message": f"Directory not found: {table_dir}",
                "elapsed_time": 0
            }
        
        # Get CSV files for this table type
        self.data_directory = table_dir
        self.find_csv_files()
        
        if not self.csv_files:
            self.logger.error(f"No CSV files found in {table_dir}")
            return {
                "status": "error",
                "message": f"No CSV files found in {table_dir}",
                "elapsed_time": (datetime.now() - start_time).total_seconds()
            }
        
        # Truncate the table
        await self.truncate_table(table_name)
        
        total_processed = 0
        
        for csv_file in self.csv_files:
            self.logger.info(f"Processing {table_type} file: {csv_file}")
            
            # Read and process the CSV file
            records = self.read_csv(csv_file, delimiter=',', fieldnames=fieldnames)
            
            if not records:
                self.logger.warning(f"No records found in {csv_file}")
                continue
            
            self.logger.info(f"Found {len(records)} records in {csv_file}")
            
            # Clean the records
            cleaned_records = [self.clean_record(record, table_type) for record in records]
            
            # Bulk insert the records
            inserted = await self.bulk_insert(table, cleaned_records)
            
            self.logger.info(f"Inserted {inserted} records from {csv_file}")
            total_processed += inserted
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            "status": "success" if not self.errors else "partial_success",
            "table": table_name,
            "files_processed": len(self.csv_files),
            "records_processed": total_processed,
            "errors": self.errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_processed / elapsed_time if elapsed_time > 0 else 0
        }
        
        self.logger.info(f"{table_type} import completed. {total_processed} records processed in {elapsed_time:.2f} seconds")
        
        return result
    
    async def import_data(self) -> Dict[str, Any]:
        """
        Import WOF data from CSV files to all WOF tables.
        
        Returns:
            Dictionary with import statistics for all tables.
        """
        start_time = datetime.now()
        
        # Import each table in sequence
        results = {}
        
        for table_type in ['spr', 'ancestors', 'concordances', 'geojson', 'names']:
            self.logger.info(f"Starting import for {table_type} table")
            results[table_type] = await self.import_table_data(table_type)
        
        # Calculate overall statistics
        total_records = sum(result.get('records_processed', 0) for result in results.values())
        total_files = sum(result.get('files_processed', 0) for result in results.values())
        total_errors = sum(len(result.get('errors', [])) for result in results.values())
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        overall_result = {
            "status": "success" if total_errors == 0 else "partial_success",
            "tables_processed": len(results),
            "files_processed": total_files,
            "records_processed": total_records,
            "errors_count": total_errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_records / elapsed_time if elapsed_time > 0 else 0,
            "table_results": results
        }
        
        self.logger.info(f"WOF import completed. {total_records} records processed across {len(results)} tables in {elapsed_time:.2f} seconds")
        
        return overall_result

# Run this module directly to test the importer
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def run_import():
        importer = WofImporter()
        result = await importer.import_data()
        print(json.dumps(result, indent=2))
    
    asyncio.run(run_import()) 