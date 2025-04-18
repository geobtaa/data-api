import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict

from db.models import gazetteer_btaa

from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class BtaaImporter(BaseImporter):
    """Importer for BTAA gazetteer data."""

    # BTAA-specific data directory
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data",
        "gazetteers",
        "btaa",
    )

    # Map CSV column names to database field names
    FIELD_MAPPING = {
        "Fast": "fast_area",
        "Bounding Box": "bounding_box",
        "Geometry": "geometry",
        "GeoNames ID": "geonames_id",
        "State Abbv": "state_abbv",
        "State Name": "state_name",
        "County_FIPS": "county_fips",
        "STATEFP": "statefp",
        "NAMELSAD": "namelsad",
    }

    # Smaller chunk size to avoid PostgreSQL parameter limits (similar to GeoNames)
    # The BTAA table has 9 fields + 2 for created_at/updated_at, so 11 params per record
    # 32767 / 11 ≈ 2979, using 2000 to be safe
    CHUNK_SIZE = 2000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get("data_directory") or self.DATA_DIR
        self.table = gazetteer_btaa
        self.table_name = "gazetteer_btaa"

    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a BTAA record before insertion.

        Args:
            record: The raw record from the CSV.

        Returns:
            Cleaned record ready for database insertion.
        """
        # Rename fields based on the mapping
        cleaned_record = {}
        for csv_field, db_field in self.FIELD_MAPPING.items():
            if csv_field in record:
                cleaned_record[db_field] = record[csv_field]

        # Call the parent method to handle common cleaning
        cleaned_record = super().clean_record(cleaned_record)

        # Handle required fields
        if not cleaned_record.get("fast_area"):
            cleaned_record["fast_area"] = "Unknown"

        return cleaned_record

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
                "elapsed_time": (datetime.now() - start_time).total_seconds(),
            }

        # Truncate the table if it exists
        await self.truncate_table(self.table_name)

        total_processed = 0

        for csv_file in self.csv_files:
            file_start_time = datetime.now()
            self.logger.info(f"Processing file: {csv_file}")

            # Read and process the CSV file
            # Don't provide fieldnames - we'll use the header row
            records = self.read_csv(csv_file)

            if not records:
                self.logger.warning(f"No records found in {csv_file}")
                continue

            total_records = len(records)
            self.logger.info(f"Found {total_records} records in {csv_file}")

            # Clean the records
            self.logger.info("Cleaning records...")
            cleaned_records = []
            for record in records:
                cleaned_record = self.clean_record(record)
                if cleaned_record is not None:
                    cleaned_records.append(cleaned_record)

            self.logger.info(f"Cleaned {len(cleaned_records)} records")

            # Bulk insert the records with progress reporting
            self.logger.info(f"Inserting records using chunk size of {self.CHUNK_SIZE}...")
            chunks = self.chunk_data(cleaned_records, self.CHUNK_SIZE)
            total_chunks = len(chunks)

            inserted = 0
            for i, chunk in enumerate(chunks):
                if i > 0 and i % 5 == 0:  # Log progress every 5 chunks
                    elapsed = (datetime.now() - file_start_time).total_seconds()
                    progress = (i / total_chunks) * 100
                    records_per_sec = inserted / elapsed if elapsed > 0 else 0
                    self.logger.info(
                        f"Progress: {progress:.1f}% - Inserted {inserted:,} of {len(cleaned_records):,} records ({records_per_sec:.1f} records/sec)"
                    )

                chunk_inserted = await self.bulk_insert(self.table, chunk)
                inserted += chunk_inserted

            file_elapsed_time = (datetime.now() - file_start_time).total_seconds()
            records_per_second = inserted / file_elapsed_time if file_elapsed_time > 0 else 0

            self.logger.info(
                f"Inserted {inserted:,} records from {csv_file} in {file_elapsed_time:.2f} seconds ({records_per_second:.1f} records/sec)"
            )
            total_processed += inserted

        elapsed_time = (datetime.now() - start_time).total_seconds()

        result = {
            "status": "success" if not self.errors else "partial_success",
            "files_processed": len(self.csv_files),
            "records_processed": total_processed,
            "errors": self.errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_processed / elapsed_time if elapsed_time > 0 else 0,
        }

        self.logger.info(
            f"Import completed. {total_processed:,} records processed in {elapsed_time:.2f} seconds ({result['records_per_second']:.1f} records/sec)"
        )

        return result


# Run this module directly to test the importer
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def run_import():
        importer = BtaaImporter()
        result = await importer.import_data()
        print(result)

    asyncio.run(run_import())
