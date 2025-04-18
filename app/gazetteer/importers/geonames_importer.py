import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from db.models import gazetteer_geonames

from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class GeonamesImporter(BaseImporter):
    """Importer for GeoNames data."""

    # GeoNames-specific data directory
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data",
        "gazetteers",
        "geonames",
    )

    # GeoNames field names in order
    # Based on https://download.geonames.org/export/dump/readme.txt
    FIELDNAMES = [
        "geonameid",
        "name",
        "asciiname",
        "alternatenames",
        "latitude",
        "longitude",
        "feature_class",
        "feature_code",
        "country_code",
        "cc2",
        "admin1_code",
        "admin2_code",
        "admin3_code",
        "admin4_code",
        "population",
        "elevation",
        "dem",
        "timezone",
        "modification_date",
    ]

    # Smaller chunk size for GeoNames to avoid exceeding PostgreSQL parameter limits
    # Each record has 19 fields + 2 for created_at/updated_at, so 21 params per record
    # 32767 / 21 ≈ 1560, using 1500 to be safe
    CHUNK_SIZE = 1500

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get("data_directory") or self.DATA_DIR
        self.table = gazetteer_geonames
        self.table_name = "gazetteer_geonames"

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

        # Handle required fields with null values
        # For asciiname, use the name field if asciiname is null
        if record.get("asciiname") is None or record.get("asciiname") == "":
            if record.get("name"):
                record["asciiname"] = record["name"]
            else:
                record["asciiname"] = "Unknown"  # Fallback default

        # For name, use asciiname or a default if null
        if record.get("name") is None or record.get("name") == "":
            if record.get("asciiname"):
                record["name"] = record["asciiname"]
            else:
                record["name"] = "Unknown"  # Fallback default

        # Ensure latitude and longitude are present (required fields)
        if record.get("latitude") is None or record.get("latitude") == "":
            record["latitude"] = 0.0  # Default to 0,0 coordinates

        if record.get("longitude") is None or record.get("longitude") == "":
            record["longitude"] = 0.0  # Default to 0,0 coordinates

        # Convert numeric fields
        try:
            # Integer conversions
            for int_field in ["geonameid", "population", "elevation", "dem"]:
                if record.get(int_field) and record[int_field] not in ["", None]:
                    record[int_field] = int(record[int_field])
                else:
                    record[int_field] = None

            # Decimal conversions
            for decimal_field in ["latitude", "longitude"]:
                if record.get(decimal_field) and record[decimal_field] not in ["", None]:
                    record[decimal_field] = float(record[decimal_field])
                else:
                    # Use defaults for required fields
                    record[decimal_field] = 0.0

            # Date conversion
            if record.get("modification_date") and record["modification_date"] not in ["", None]:
                # GeoNames date format is YYYY-MM-DD
                record["modification_date"] = datetime.strptime(
                    record["modification_date"], "%Y-%m-%d"
                ).date()
            else:
                record["modification_date"] = None

            # Ensure geonameid exists and is valid (primary key requirement)
            if record.get("geonameid") is None:
                # Skip this record to avoid primary key issues
                self.logger.warning("Skipping record with null geonameid")
                return None

        except (ValueError, TypeError) as e:
            self.logger.warning(f"Error converting field in record {record.get('geonameid')}: {e}")

        return record

    def find_text_files(self) -> List[str]:
        """Find text files (.txt) in the specified directory."""
        if not os.path.exists(self.data_directory):
            self.logger.error(f"Data directory does not exist: {self.data_directory}")
            return []

        txt_files = []
        for root, _, files in os.walk(self.data_directory):
            for file in files:
                if file.lower().endswith(".txt"):
                    txt_files.append(os.path.join(root, file))

        self.txt_files = txt_files
        self.logger.info(f"Found {len(txt_files)} text files")
        return txt_files

    async def import_data(self) -> Dict[str, Any]:
        """
        Import GeoNames data from txt files to the database.

        Returns:
            Dictionary with import statistics.
        """
        start_time = datetime.now()
        self.txt_files = self.find_text_files()

        if not self.txt_files:
            self.logger.error(f"No text files (.txt) found in {self.data_directory}")
            return {
                "status": "error",
                "message": f"No text files (.txt) found in {self.data_directory}",
                "elapsed_time": (datetime.now() - start_time).total_seconds(),
            }

        # Truncate the table if it exists
        await self.truncate_table(self.table_name)

        total_processed = 0
        skipped_records = 0

        for txt_file in self.txt_files:
            file_start_time = datetime.now()
            self.logger.info(f"Processing file: {txt_file}")

            # Read and process the TXT file
            # GeoNames files are tab-delimited
            records = self.read_csv(txt_file, delimiter="\t", fieldnames=self.FIELDNAMES)

            if not records:
                self.logger.warning(f"No records found in {txt_file}")
                continue

            total_records = len(records)
            self.logger.info(f"Found {total_records} records in {txt_file}")

            # Clean the records
            self.logger.info("Cleaning records...")
            cleaned_records = []
            for record in records:
                cleaned_record = self.clean_record(record)
                if cleaned_record is not None:
                    cleaned_records.append(cleaned_record)
                else:
                    skipped_records += 1

            self.logger.info(
                f"Cleaned {len(cleaned_records)} records, skipped {skipped_records} invalid records"
            )

            # Bulk insert the records with progress reporting
            self.logger.info(f"Inserting records using chunk size of {self.CHUNK_SIZE}...")
            chunks = self.chunk_data(cleaned_records, self.CHUNK_SIZE)
            total_chunks = len(chunks)

            inserted = 0
            for i, chunk in enumerate(chunks):
                if i > 0 and i % 10 == 0:  # Log progress every 10 chunks
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
                f"Inserted {inserted:,} records from {txt_file} in {file_elapsed_time:.2f} seconds ({records_per_second:.1f} records/sec)"
            )
            total_processed += inserted

        elapsed_time = (datetime.now() - start_time).total_seconds()

        result = {
            "status": "success" if not self.errors else "partial_success",
            "files_processed": len(self.txt_files),
            "records_processed": total_processed,
            "records_skipped": skipped_records,
            "errors": self.errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_processed / elapsed_time if elapsed_time > 0 else 0,
        }

        self.logger.info(
            f"Import completed. {total_processed:,} records processed, {skipped_records} records skipped in {elapsed_time:.2f} seconds ({result['records_per_second']:.1f} records/sec)"
        )

        return result


# Run this module directly to test the importer
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def run_import():
        importer = GeonamesImporter()
        result = await importer.import_data()
        print(result)

    asyncio.run(run_import())
