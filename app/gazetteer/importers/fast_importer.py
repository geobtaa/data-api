import asyncio
import csv
import logging
import os
import re
import tempfile
import zipfile
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.request import urlretrieve
import xml.sax
import xml.sax.handler

"""
FAST (Faceted Application of Subject Terminology) Importer

This module imports geographic entries from OCLC's FAST dataset.
Data source: OCLC ResearchWorks (https://researchworks.oclc.org/researchdata/fast/)
Attribution: OCLC FAST data is provided by OCLC under the OCLC ResearchWorks license.
"""

from db.models import gazetteer_fast

from .base_importer import BaseImporter

logger = logging.getLogger(__name__)


class FastMarcXmlHandler(xml.sax.handler.ContentHandler):
    """SAX handler for parsing MARCXML records from the FAST dataset."""

    def __init__(self):
        """Initialize the handler."""
        super().__init__()
        self.records = []
        self.current_record = {}
        self.current_field = None
        self.current_subfield = None
        self.current_value = ""
        self.in_record = False
        self.in_controlfield = False
        self.in_datafield = False
        self.in_subfield = False
        self.logger = logging.getLogger(__name__)
        self.record_count = 0
        self.field_count = 0
        self.ind1 = None
        self.ind2 = None

    def startElement(self, name, attrs):
        """Handle start element events."""
        # Remove namespace prefix if present
        local_name = name.split(":")[-1]
        
        if local_name == "record":
            self.in_record = True
            self.current_record = {}
            self.record_count += 1
            if self.record_count % 1000 == 0:
                self.logger.info(f"Parsed {self.record_count} records so far")
        elif local_name == "controlfield" and self.in_record:
            self.in_controlfield = True
            self.current_field = attrs.get("tag")
            self.current_value = ""
        elif local_name == "datafield" and self.in_record:
            self.in_datafield = True
            self.current_field = attrs.get("tag")
            self.ind1 = attrs.get("ind1", " ")
            self.ind2 = attrs.get("ind2", " ")
            self.field_count += 1
            if self.field_count % 10000 == 0:
                self.logger.info(f"Parsed {self.field_count} fields so far")
        elif local_name == "subfield" and self.in_datafield:
            self.in_subfield = True
            self.current_subfield = attrs.get("code")
            self.current_value = ""

    def endElement(self, name):
        """Handle end element events."""
        # Remove namespace prefix if present
        local_name = name.split(":")[-1]
        
        if local_name == "record":
            self.in_record = False
            self.records.append(self.current_record)
            if len(self.records) % 1000 == 0:
                self.logger.info(f"Added {len(self.records)} records to the list")
        elif local_name == "controlfield" and self.in_record:
            self.in_controlfield = False
            if self.current_field and self.current_value:
                field_key = f"{self.current_field}"
                if field_key not in self.current_record:
                    self.current_record[field_key] = []
                self.current_record[field_key].append(self.current_value)
        elif local_name == "datafield" and self.in_record:
            self.in_datafield = False
            self.ind1 = None
            self.ind2 = None
        elif local_name == "subfield" and self.in_datafield:
            self.in_subfield = False
            if self.current_field and self.current_subfield and self.current_value:
                # Include indicator values in the field key
                field_key = f"{self.current_field}_{self.ind1}_{self.ind2}_{self.current_subfield}"
                if field_key not in self.current_record:
                    self.current_record[field_key] = []
                self.current_record[field_key].append(self.current_value)

    def characters(self, content):
        """Handle character events."""
        if self.in_controlfield or self.in_subfield:
            self.current_value += content

    def endDocument(self):
        """Handle end document events."""
        self.logger.info(f"Finished parsing document. Total records: {len(self.records)}")
        if self.records:
            self.logger.info(f"Sample record keys: {list(self.records[0].keys())}")
            if "016" in self.records[0]:
                self.logger.info(f"Sample FAST ID: {self.records[0]['016']}")
            if "024" in self.records[0]:
                self.logger.info(f"Sample URI: {self.records[0]['024']}")
            if "151" in self.records[0]:
                self.logger.info(f"Sample label: {self.records[0]['151']}")


class FastImporter(BaseImporter):
    """Importer for OCLC FAST Dataset Geographic entries."""

    # FAST-specific data directory
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data",
        "gazetteers",
        "fast",
    )

    # URL for the FAST Geographic MARCXML dataset
    FAST_URL = "https://researchworks.oclc.org/researchdata/fast/FASTGeographic.marcxml.zip"

    # CSV field names
    FIELDNAMES = ["fast_id", "uri", "type", "label", "geoname_id", "viaf_id", "wikipedia_id"]
    
    # Reduced chunk size to avoid PostgreSQL parameter limit (32,767)
    # Each record has 6 fields, so we need to keep chunk_size * 6 < 32767
    CHUNK_SIZE = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.data_directory = kwargs.get("data_directory") or self.DATA_DIR
        self.table = gazetteer_fast
        self.table_name = "gazetteer_fast"
        self.marcxml_file = None
        self.csv_file = None

    def download_and_extract(self) -> str:
        """
        Download and extract the FAST MARCXML dataset.

        Returns:
            Path to the extracted MARCXML file.
        """
        # Create data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)

        # Create a temporary directory for extraction
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "FASTGeographic.marcxml.zip")
            
            # Download the file
            self.logger.info(f"Downloading FAST dataset from {self.FAST_URL}")
            try:
                urlretrieve(self.FAST_URL, zip_path)
                self.logger.info(f"Downloaded file to {zip_path}")
                
                # Check if the file was downloaded successfully
                if not os.path.exists(zip_path):
                    raise FileNotFoundError(f"Failed to download file from {self.FAST_URL}")
                
                file_size = os.path.getsize(zip_path)
                self.logger.info(f"Downloaded file size: {file_size} bytes")
                
                if file_size == 0:
                    raise ValueError("Downloaded file is empty")
            except Exception as e:
                self.logger.error(f"Error downloading file: {e}")
                raise
            
            # Extract the file
            self.logger.info("Extracting MARCXML file")
            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    # List all files in the zip
                    all_files = zip_ref.namelist()
                    self.logger.info(f"Files in ZIP: {all_files}")
                    
                    # Find the MARCXML file in the zip
                    marcxml_files = [f for f in all_files if f.endswith(".marcxml")]
                    if not marcxml_files:
                        raise FileNotFoundError("No MARCXML file found in the ZIP archive")
                    
                    # Extract the first MARCXML file
                    marcxml_filename = marcxml_files[0]
                    self.logger.info(f"Extracting {marcxml_filename} from ZIP")
                    zip_ref.extract(marcxml_filename, temp_dir)
                    
                    # Move the extracted file to the data directory
                    extracted_path = os.path.join(temp_dir, marcxml_filename)
                    target_path = os.path.join(self.data_directory, "FASTGeographic.marcxml")
                    
                    # Copy the file to the data directory
                    with open(extracted_path, "rb") as src, open(target_path, "wb") as dst:
                        dst.write(src.read())
                    
                    # Check if the file was copied successfully
                    if not os.path.exists(target_path):
                        raise FileNotFoundError(f"Failed to copy file to {target_path}")
                    
                    file_size = os.path.getsize(target_path)
                    self.logger.info(f"Extracted MARCXML file size: {file_size} bytes")
                    
                    if file_size == 0:
                        raise ValueError("Extracted MARCXML file is empty")
                    
                    self.logger.info(f"Extracted MARCXML file to {target_path}")
                    return target_path
            except Exception as e:
                self.logger.error(f"Error extracting file: {e}")
                raise

    def parse_marcxml(self, marcxml_file: str) -> List[Dict[str, Any]]:
        """
        Parse the MARCXML file using SAX.

        Args:
            marcxml_file: Path to the MARCXML file.

        Returns:
            List of parsed records.
        """
        self.logger.info(f"Parsing MARCXML file: {marcxml_file}")
        
        # Check if the file exists
        if not os.path.exists(marcxml_file):
            self.logger.error(f"MARCXML file does not exist: {marcxml_file}")
            return []
        
        # Check file size
        file_size = os.path.getsize(marcxml_file)
        self.logger.info(f"MARCXML file size: {file_size} bytes")
        
        if file_size == 0:
            self.logger.error("MARCXML file is empty")
            return []
        
        # Create a SAX parser
        parser = xml.sax.make_parser()
        handler = FastMarcXmlHandler()
        parser.setContentHandler(handler)
        
        # Parse the file
        try:
            parser.parse(marcxml_file)
            self.logger.info(f"Parsed {len(handler.records)} records from MARCXML file")
            
            # Log a sample of the first record if available
            if handler.records:
                self.logger.info(f"Sample record keys: {list(handler.records[0].keys())}")
                if "016" in handler.records[0]:
                    self.logger.info(f"Sample FAST ID: {handler.records[0]['016']}")
                if "024" in handler.records[0]:
                    self.logger.info(f"Sample URI: {handler.records[0]['024']}")
                if "151" in handler.records[0]:
                    self.logger.info(f"Sample label: {handler.records[0]['151']}")
            
            return handler.records
        except Exception as e:
            self.logger.error(f"Error parsing MARCXML file: {e}")
            return []

    def process_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process the parsed MARCXML records into the required format.

        Args:
            records: List of parsed MARCXML records.

        Returns:
            List of processed records ready for CSV export.
        """
        processed_records = []
        
        self.logger.info(f"Processing {len(records)} records")
        
        for i, record in enumerate(records):
            # Log progress every 1000 records
            if i > 0 and i % 1000 == 0:
                self.logger.info(f"Processed {i} records so far")
            
            # Extract FAST ID from 016 field
            fast_id = None
            for key in record:
                if key.startswith("016_"):
                    # Extract the numeric part after "fst" and remove leading zeros
                    for value in record[key]:
                        match = re.match(r"fst(\d+)", value)
                        if match:
                            fast_id = str(int(match.group(1)))  # Convert to int to remove leading zeros
                            self.logger.debug(f"Extracted FAST ID: {fast_id} from {value}")
                            break
                    if fast_id:
                        break
            
            # Construct URI from FAST ID
            fast_uri = f"https://id.worldcat.org/fast/{fast_id}" if fast_id else None
            
            # Type is always "place" for geographic entries
            type_value = "place"
            
            # Extract label from 151 field
            label = None
            label_parts = []
            for key in record:
                if key.startswith("151_"):
                    label_parts.extend(record[key])
            
            if label_parts:
                label = "--".join(label_parts)
                self.logger.debug(f"Extracted label: {label}")
            
            # Extract GeoNames ID from 751 field
            geoname_id = None
            for key in record:
                if key.startswith("751_"):
                    for uri in record[key]:
                        # Check for GeoNames ID
                        geonames_match = re.search(r"geonames\.org/(\d+)", uri)
                        if geonames_match:
                            geoname_id = geonames_match.group(1)
                            self.logger.debug(f"Extracted GeoNames ID: {geoname_id} from {uri}")
                            break
                    if geoname_id:
                        break
            
            # Extract VIAF ID from 751 field
            viaf_id = None
            for key in record:
                if key.startswith("751_"):
                    for uri in record[key]:
                        # Check for VIAF ID
                        viaf_match = re.search(r"viaf\.org/viaf/(\d+)", uri)
                        if viaf_match:
                            viaf_id = viaf_match.group(1)
                            self.logger.debug(f"Extracted VIAF ID: {viaf_id} from {uri}")
                            break
                    if viaf_id:
                        break
            
            # Extract Wikipedia page identifier from 751 field
            wikipedia_id = None
            for key in record:
                if key.startswith("751_"):
                    for uri in record[key]:
                        # Check for Wikipedia URL
                        wiki_match = re.search(r"wikipedia\.org/wiki/([^/]+)", uri)
                        if wiki_match:
                            wikipedia_id = wiki_match.group(1)
                            self.logger.debug(f"Extracted Wikipedia ID: {wikipedia_id} from {uri}")
                            break
                    if wikipedia_id:
                        break
            
            # Only add records with required fields
            if fast_id and fast_uri and label:
                processed_records.append({
                    "fast_id": fast_id,
                    "uri": fast_uri,
                    "type": type_value,
                    "label": label,
                    "geoname_id": geoname_id,
                    "viaf_id": viaf_id,
                    "wikipedia_id": wikipedia_id
                })
            else:
                self.logger.debug(f"Skipping record due to missing required fields: fast_id={fast_id}, uri={uri}, label={label}")
        
        self.logger.info(f"Processed {len(processed_records)} records")
        
        # Log a sample of the first processed record if available
        if processed_records:
            self.logger.info(f"Sample processed record: {processed_records[0]}")
        
        return processed_records

    def export_to_csv(self, records: List[Dict[str, Any]]) -> str:
        """
        Export the processed records to a CSV file.

        Args:
            records: List of processed records.

        Returns:
            Path to the exported CSV file.
        """
        csv_path = os.path.join(self.data_directory, "fast_geographic.csv")
        
        with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=self.FIELDNAMES)
            writer.writeheader()
            writer.writerows(records)
        
        self.logger.info(f"Exported {len(records)} records to {csv_path}")
        return csv_path

    def clean_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean and transform a FAST record before insertion.

        Args:
            record: The raw record from the CSV.

        Returns:
            Cleaned record ready for database insertion.
        """
        # Call the parent method to handle common cleaning
        record = super().clean_record(record)
        
        # Ensure required fields are present
        if not record.get("fast_id"):
            self.logger.warning("Skipping record with null fast_id")
            return None
        
        if not record.get("uri"):
            self.logger.warning(f"Skipping record {record.get('fast_id')} with null uri")
            return None
        
        if not record.get("type"):
            record["type"] = "place"  # Default type
        
        if not record.get("label"):
            self.logger.warning(f"Skipping record {record.get('fast_id')} with null label")
            return None
        
        return record

    async def import_data(self) -> Dict[str, Any]:
        """
        Import FAST data from MARCXML to the database.

        Returns:
            Dictionary with import statistics.
        """
        start_time = datetime.now()
        
        try:
            # Download and extract the MARCXML file
            marcxml_file = self.download_and_extract()
            self.marcxml_file = marcxml_file
            
            # Parse the MARCXML file
            records = self.parse_marcxml(marcxml_file)
            
            # Process the records
            processed_records = self.process_records(records)
            
            # Export to CSV for verification
            csv_file = self.export_to_csv(processed_records)
            self.csv_file = csv_file
            
            # Truncate the table if it exists
            await self.truncate_table(self.table_name)
            
            # Clean the records
            self.logger.info("Cleaning records...")
            cleaned_records = []
            for record in processed_records:
                cleaned_record = self.clean_record(record)
                if cleaned_record is not None:
                    cleaned_records.append(cleaned_record)
            
            self.logger.info(f"Cleaned {len(cleaned_records)} records")
            
            # Bulk insert the records
            self.logger.info(f"Inserting records using chunk size of {self.CHUNK_SIZE}...")
            chunks = self.chunk_data(cleaned_records, self.CHUNK_SIZE)
            total_chunks = len(chunks)
            
            inserted = 0
            for i, chunk in enumerate(chunks):
                if i > 0 and i % 10 == 0:  # Log progress every 10 chunks
                    elapsed = (datetime.now() - start_time).total_seconds()
                    progress = (i / total_chunks) * 100
                    records_per_sec = inserted / elapsed if elapsed > 0 else 0
                    self.logger.info(
                        f"Progress: {progress:.1f}% - Inserted {inserted:,} of "
                        f"{len(cleaned_records):,} records ({records_per_sec:.1f} records/sec)"
                    )
                
                chunk_inserted = await self.bulk_insert(self.table, chunk)
                inserted += chunk_inserted
                
                # Add a small delay between chunks to prevent overwhelming the database
                if i < total_chunks - 1:  # Don't delay after the last chunk
                    await asyncio.sleep(0.1)  # 100ms delay between chunks
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "status": "success" if not self.errors else "partial_success",
                "records_processed": len(processed_records),
                "records_inserted": inserted,
                "errors": self.errors,
                "elapsed_time": elapsed_time,
                "records_per_second": inserted / elapsed_time if elapsed_time > 0 else 0,
            }
            
            self.logger.info(
                f"Import completed. {inserted:,} records processed in {elapsed_time:.2f} seconds "
                f"({result['records_per_second']:.1f} records/sec)"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error during import: {e}")
            self.errors.append({"operation": "import", "error": str(e)})
            
            elapsed_time = (datetime.now() - start_time).total_seconds()
            
            return {
                "status": "error",
                "message": str(e),
                "elapsed_time": elapsed_time,
            }


# Run this module directly to test the importer
if __name__ == "__main__":
    async def run_import():
        importer = FastImporter()
        result = await importer.import_data()
        print(result)

    asyncio.run(run_import()) 