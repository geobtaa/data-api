import asyncio
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict

from db.models import (
    gazetteer_wof_ancestors,
    gazetteer_wof_concordances,
    gazetteer_wof_geojson,
    gazetteer_wof_names,
    gazetteer_wof_spr,
)

from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class WofImporter(BaseImporter):
    """Importer for Who's on First (WOF) data."""

    # WOF-specific data directory
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data",
        "gazetteers",
        "wof",
    )

    # Smaller chunk size to avoid PostgreSQL parameter limits
    # Using a very conservative value that should work for all WOF tables
    # The SPR table has the most fields (20) + 2 for created_at/updated_at
    # 32767 / 22 ≈ 1489, using 1000 to be safe
    CHUNK_SIZE = 1000

    # WOF field mappings for each table
    SPR_FIELDNAMES = [
        "wok_id",
        "parent_id",
        "name",
        "placetype",
        "country",
        "repo",
        "latitude",
        "longitude",
        "min_latitude",
        "min_longitude",
        "max_latitude",
        "max_longitude",
        "is_current",
        "is_deprecated",
        "is_ceased",
        "is_superseded",
        "is_superseding",
        "superseded_by",
        "supersedes",
        "lastmodified",
    ]

    ANCESTORS_FIELDNAMES = ["wok_id", "ancestor_id", "ancestor_placetype", "lastmodified"]

    CONCORDANCES_FIELDNAMES = ["wok_id", "other_id", "other_source", "lastmodified"]

    GEOJSON_FIELDNAMES = ["wok_id", "body", "source", "alt_label", "is_alt", "lastmodified"]

    NAMES_FIELDNAMES = [
        "wok_id",
        "placetype",
        "country",
        "language",
        "extlang",
        "script",
        "region",
        "variant",
        "extension",
        "privateuse",
        "name",
        "lastmodified",
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get("data_directory") or self.DATA_DIR

        # Dictionary of tables
        self.tables = {
            "spr": {
                "table": gazetteer_wof_spr,
                "name": "gazetteer_wof_spr",
                "fieldnames": self.SPR_FIELDNAMES,
            },
            "ancestors": {
                "table": gazetteer_wof_ancestors,
                "name": "gazetteer_wof_ancestors",
                "fieldnames": self.ANCESTORS_FIELDNAMES,
            },
            "concordances": {
                "table": gazetteer_wof_concordances,
                "name": "gazetteer_wof_concordances",
                "fieldnames": self.CONCORDANCES_FIELDNAMES,
            },
            "geojson": {
                "table": gazetteer_wof_geojson,
                "name": "gazetteer_wof_geojson",
                "fieldnames": self.GEOJSON_FIELDNAMES,
            },
            "names": {
                "table": gazetteer_wof_names,
                "name": "gazetteer_wof_names",
                "fieldnames": self.NAMES_FIELDNAMES,
            },
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

        # Skip header row if it somehow got included
        if record.get("wok_id") == "id" or record.get("wok_id") == "wok_id":
            self.logger.warning(f"Skipping header row in {table_type} data")
            return None

        try:
            # Handle specific conversions based on table type
            if table_type == "spr":
                # Handle missing name - set default value for not-null constraint
                if record.get("name") is None or record.get("name") == "":
                    record["name"] = (
                        f"Unnamed {record.get('placetype', 'place')} #{record.get('wok_id', 'unknown')}"
                    )
                    self.logger.warning(
                        f"Found record with null name, using fallback: {record['name']}"
                    )

                # BigInteger conversions
                for big_int_field in ["wok_id", "parent_id"]:
                    if record.get(big_int_field) and record[big_int_field] not in ["", None]:
                        record[big_int_field] = int(record[big_int_field])
                    else:
                        record[big_int_field] = None

                # Handle fields that could be comma-separated lists
                # For these fields, we'll take just the first value
                for list_field in ["superseded_by", "supersedes"]:
                    if record.get(list_field) and record[list_field] not in ["", None]:
                        # Check if it's a comma-separated list
                        if "," in record[list_field]:
                            # Get the first value
                            first_val = record[list_field].split(",")[0]
                            try:
                                record[list_field] = int(first_val)
                            except (ValueError, TypeError):
                                record[list_field] = None
                        else:
                            try:
                                record[list_field] = int(record[list_field])
                            except (ValueError, TypeError):
                                record[list_field] = None
                    else:
                        record[list_field] = None

                # Regular integer conversions for other fields
                for int_field in [
                    "is_current",
                    "is_deprecated",
                    "is_ceased",
                    "is_superseded",
                    "is_superseding",
                    "lastmodified",
                ]:
                    if record.get(int_field) and record[int_field] not in ["", None]:
                        try:
                            record[int_field] = int(record[int_field])
                        except (ValueError, TypeError):
                            record[int_field] = None
                    else:
                        record[int_field] = None

                # Decimal conversions
                for decimal_field in [
                    "latitude",
                    "longitude",
                    "min_latitude",
                    "min_longitude",
                    "max_latitude",
                    "max_longitude",
                ]:
                    if record.get(decimal_field) and record[decimal_field] not in ["", None]:
                        try:
                            record[decimal_field] = float(record[decimal_field])
                        except (ValueError, TypeError):
                            record[decimal_field] = None
                    else:
                        record[decimal_field] = None

            elif table_type == "ancestors":
                # Skip header row
                if record.get("wok_id") == "id":
                    return None

                # Check for required fields - skip record if ancestor_id is null
                if record.get("ancestor_id") is None or record.get("ancestor_id") == "":
                    self.logger.warning(
                        f"Skipping ancestor record for wok_id {record.get('wok_id')} due to null ancestor_id"
                    )
                    return None

                # BigInteger and Integer conversions
                if record.get("wok_id") and record["wok_id"] not in ["", None]:
                    try:
                        record["wok_id"] = int(record["wok_id"])
                    except (ValueError, TypeError):
                        record["wok_id"] = None
                else:
                    record["wok_id"] = None

                if record.get("ancestor_id") and record["ancestor_id"] not in ["", None]:
                    try:
                        record["ancestor_id"] = int(record["ancestor_id"])
                    except (ValueError, TypeError):
                        record["ancestor_id"] = None
                else:
                    record["ancestor_id"] = None

                if record.get("lastmodified") and record["lastmodified"] not in ["", None]:
                    try:
                        record["lastmodified"] = int(record["lastmodified"])
                    except (ValueError, TypeError):
                        record["lastmodified"] = None
                else:
                    record["lastmodified"] = None

            elif table_type == "concordances":
                # Skip header row
                if record.get("wok_id") == "id":
                    return None

                # Check for required fields - skip record if other_id is null
                if record.get("other_id") is None or record.get("other_id") == "":
                    self.logger.warning(
                        f"Skipping concordance record for wok_id {record.get('wok_id')} due to null other_id"
                    )
                    return None

                # BigInteger conversion
                if record.get("wok_id") and record["wok_id"] not in ["", None]:
                    try:
                        record["wok_id"] = int(record["wok_id"])
                    except (ValueError, TypeError):
                        record["wok_id"] = None
                else:
                    record["wok_id"] = None

                if record.get("lastmodified") and record["lastmodified"] not in ["", None]:
                    try:
                        record["lastmodified"] = int(record["lastmodified"])
                    except (ValueError, TypeError):
                        record["lastmodified"] = None
                else:
                    record["lastmodified"] = None

            elif table_type == "geojson":
                # Skip header row
                if record.get("wok_id") == "id":
                    return None

                # Check for required fields - skip record if body is null
                if record.get("body") is None or record.get("body") == "":
                    self.logger.warning(
                        f"Skipping geojson record for wok_id {record.get('wok_id')} due to null body"
                    )
                    return None

                # BigInteger conversion
                if record.get("wok_id") and record["wok_id"] not in ["", None]:
                    try:
                        record["wok_id"] = int(record["wok_id"])
                    except (ValueError, TypeError):
                        record["wok_id"] = None
                else:
                    record["wok_id"] = None

                # Boolean conversion
                if record.get("is_alt") is not None:
                    if isinstance(record["is_alt"], str):
                        record["is_alt"] = record["is_alt"].lower() in [
                            "true",
                            "1",
                            "t",
                            "yes",
                            "y",
                        ]

                if record.get("lastmodified") and record["lastmodified"] not in ["", None]:
                    try:
                        record["lastmodified"] = int(record["lastmodified"])
                    except (ValueError, TypeError):
                        record["lastmodified"] = None
                else:
                    record["lastmodified"] = None

            elif table_type == "names":
                # Skip header row
                if record.get("wok_id") == "id":
                    return None

                # Check for required fields - skip record if name is null
                if record.get("name") is None or record.get("name") == "":
                    self.logger.warning(
                        f"Skipping names record for wok_id {record.get('wok_id')} due to null name"
                    )
                    return None

                # BigInteger conversion
                if record.get("wok_id") and record["wok_id"] not in ["", None]:
                    try:
                        record["wok_id"] = int(record["wok_id"])
                    except (ValueError, TypeError):
                        record["wok_id"] = None
                else:
                    record["wok_id"] = None

                if record.get("lastmodified") and record["lastmodified"] not in ["", None]:
                    try:
                        record["lastmodified"] = int(record["lastmodified"])
                    except (ValueError, TypeError):
                        record["lastmodified"] = None
                else:
                    record["lastmodified"] = None

        except (ValueError, TypeError) as e:
            self.logger.warning(
                f"Error converting field in {table_type} record {record.get('wok_id')}: {e}"
            )

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
                "elapsed_time": 0,
            }

        table = table_info["table"]
        table_name = table_info["name"]
        fieldnames = table_info["fieldnames"]

        # Look for the specific CSV file in the csv subdirectory
        csv_dir = os.path.join(self.data_directory, "csv")
        csv_file = os.path.join(csv_dir, f"{table_type}.csv")

        if not os.path.exists(csv_file):
            self.logger.warning(f"CSV file not found for {table_type}: {csv_file}")
            return {
                "status": "warning",
                "message": f"CSV file not found: {csv_file}",
                "elapsed_time": 0,
            }

        # Truncate the table
        await self.truncate_table(table_name)

        total_processed = 0
        file_start_time = datetime.now()
        self.logger.info(f"Processing {table_type} file: {csv_file}")

        # Read and process the CSV file
        records = self.read_csv(csv_file, delimiter=",", fieldnames=fieldnames)

        if not records:
            self.logger.warning(f"No records found in {csv_file}")
            return {
                "status": "warning",
                "message": f"No records found in {csv_file}",
                "elapsed_time": (datetime.now() - start_time).total_seconds(),
            }

        total_records = len(records)
        self.logger.info(f"Found {total_records} records in {csv_file}")

        # Clean the records
        self.logger.info(f"Cleaning {table_type} records...")
        cleaned_records = []
        for record in records:
            cleaned_record = self.clean_record(record, table_type)
            if cleaned_record is not None:
                cleaned_records.append(cleaned_record)

        self.logger.info(f"Cleaned {len(cleaned_records)} {table_type} records")

        # Use a smaller chunk size for spr table since it has more fields
        chunk_size = 500 if table_type == "spr" else self.CHUNK_SIZE

        # Bulk insert the records with progress reporting
        self.logger.info(f"Inserting {table_type} records using chunk size of {chunk_size}...")
        chunks = self.chunk_data(cleaned_records, chunk_size)
        total_chunks = len(chunks)

        inserted = 0
        for i, chunk in enumerate(chunks):
            if i > 0 and i % 5 == 0:  # Log progress every 5 chunks
                elapsed = (datetime.now() - file_start_time).total_seconds()
                progress = (i / total_chunks) * 100
                records_per_sec = inserted / elapsed if elapsed > 0 else 0
                self.logger.info(
                    f"{table_type} Progress: {progress:.1f}% - Inserted {inserted:,} of {len(cleaned_records):,} records ({records_per_sec:.1f} records/sec)"
                )

            chunk_inserted = await self.bulk_insert(table, chunk)
            inserted += chunk_inserted

        file_elapsed_time = (datetime.now() - file_start_time).total_seconds()
        records_per_second = inserted / file_elapsed_time if file_elapsed_time > 0 else 0

        self.logger.info(
            f"Inserted {inserted:,} {table_type} records from {csv_file} in {file_elapsed_time:.2f} seconds ({records_per_second:.1f} records/sec)"
        )
        total_processed += inserted

        elapsed_time = (datetime.now() - start_time).total_seconds()

        return {
            "status": "success" if not self.errors else "partial_success",
            "files_processed": 1,
            "records_processed": total_processed,
            "errors": self.errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_processed / elapsed_time if elapsed_time > 0 else 0,
        }

    async def import_data(self) -> Dict[str, Any]:
        """
        Import WOF data from CSV files to all WOF tables.

        This importer looks for the following CSV files in the WOF/csv directory:
        - spr.csv: Main WOF records
        - ancestors.csv: Ancestor relationships
        - concordances.csv: Concordances to other systems
        - geojson.csv: GeoJSON data
        - names.csv: Alternative names

        Returns:
            Dictionary with import statistics for all tables.
        """
        start_time = datetime.now()

        # Import each table in sequence
        results = {}
        available_tables = []

        # Check which CSV files exist in the csv subdirectory
        csv_dir = os.path.join(self.data_directory, "csv")
        if not os.path.exists(csv_dir):
            self.logger.error(f"CSV directory not found: {csv_dir}")
            return {
                "status": "error",
                "message": f"CSV directory not found: {csv_dir}",
                "elapsed_time": (datetime.now() - start_time).total_seconds(),
            }

        for table_type in ["spr", "ancestors", "concordances", "geojson", "names"]:
            csv_path = os.path.join(csv_dir, f"{table_type}.csv")
            if os.path.exists(csv_path):
                available_tables.append(table_type)
                self.logger.info(f"Found {table_type}.csv file: {csv_path}")
            else:
                self.logger.warning(
                    f"CSV file not found: {csv_path} - skipping {table_type} table import"
                )

        if not available_tables:
            self.logger.error("No WOF CSV files found in the data directory")
            return {
                "status": "error",
                "message": "No WOF CSV files found in the data directory",
                "elapsed_time": (datetime.now() - start_time).total_seconds(),
            }

        # Process available tables
        for table_type in available_tables:
            self.logger.info(f"Starting import for {table_type} table")
            results[table_type] = await self.import_table_data(table_type)

        # Calculate overall statistics
        total_records = sum(result.get("records_processed", 0) for result in results.values())
        total_files = sum(result.get("files_processed", 0) for result in results.values())
        total_errors = sum(len(result.get("errors", [])) for result in results.values())

        elapsed_time = (datetime.now() - start_time).total_seconds()

        overall_result = {
            "status": "success" if total_errors == 0 else "partial_success",
            "tables_processed": len(results),
            "files_processed": total_files,
            "records_processed": total_records,
            "errors_count": total_errors,
            "elapsed_time": elapsed_time,
            "records_per_second": total_records / elapsed_time if elapsed_time > 0 else 0,
            "table_results": results,
        }

        self.logger.info(
            f"WOF import completed. {total_records:,} records processed across {len(results)} tables in {elapsed_time:.2f} seconds"
        )

        return overall_result


# Run this module directly to test the importer
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def run_import():
        importer = WofImporter()
        result = await importer.import_data()
        print(json.dumps(result, indent=2))

    asyncio.run(run_import())
