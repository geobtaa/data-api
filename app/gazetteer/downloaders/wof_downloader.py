#!/usr/bin/env python3
"""
Who's on First (WOF) Gazetteer Downloader

This module downloads the Who's on First SQLite database for US admin data,
extracts the compressed file, and exports tables to CSV for import by the WofImporter.

Features:
- Downloads the WOF SQLite database from a configurable URL
- Extracts the compressed bz2 file
- Exports tables from SQLite to CSV files in the expected format
- Provides command-line interface for running individual steps or all at once

Usage:
    python -m app.gazetteer.downloaders.wof_downloader [options]

Arguments:
    --download      Download and extract the SQLite database
    --export        Export SQLite tables to CSV files
    --all           Perform all operations in sequence
"""

import os
import sys
import requests
import sqlite3
import csv
import bz2
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Fix imports to work both as a module and as a direct script
try:
    # When run as a module
    from .base_downloader import BaseDownloader
except ImportError:
    # When run directly
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
    from app.gazetteer.downloaders.base_downloader import BaseDownloader

# Setup logging
logger = logging.getLogger("wof_downloader")


class WofDownloader(BaseDownloader):
    """Downloader for Who's on First (WOF) gazetteer data."""

    # URL for downloading the SQLite database
    WOF_URL = "https://data.geocode.earth/wof/dist/sqlite/whosonfirst-data-admin-us-latest.db.bz2"

    def __init__(self, data_dir=None):
        """
        Initialize the WOF downloader.

        Args:
            data_dir: Optional path to the data directory. If not provided, will use default.
        """
        super().__init__(data_dir=data_dir, gazetteer_name="wof")

        # Additional directories
        self.csv_dir = self.data_dir / "csv"
        self.csv_dir.mkdir(parents=True, exist_ok=True)

        # Database file paths
        self.db_file_bz2 = self.data_dir / "whosonfirst-data-admin-us-latest.db.bz2"
        self.db_file = self.data_dir / "whosonfirst-data-admin-us-latest.db"

    def download(self):
        """Download the compressed SQLite database and extract it."""
        if self.db_file.exists():
            logger.info(f"SQLite database already exists at {self.db_file}")
            return

        if not self.db_file_bz2.exists():
            logger.info(f"Downloading file from {self.WOF_URL}...")
            try:
                response = requests.get(self.WOF_URL, stream=True)
                response.raise_for_status()

                # Get total file size for progress reporting
                total_size = int(response.headers.get("content-length", 0))
                downloaded = 0
                chunk_size = 8192

                with open(self.db_file_bz2, "wb") as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = downloaded / total_size * 100 if total_size > 0 else 0
                            sys.stdout.write(
                                f"\rDownloading... {progress:.1f}% ({downloaded/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB)"
                            )
                            sys.stdout.flush()

                print("\nDownload completed successfully.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download file: {e}")
                return
        else:
            logger.info(f"Compressed file already exists at {self.db_file_bz2}")

        # Extract the bz2 file
        if not self.db_file.exists():
            logger.info(f"Extracting {self.db_file_bz2} to {self.db_file}...")
            try:
                with open(self.db_file_bz2, "rb") as source, open(self.db_file, "wb") as dest:
                    dest.write(bz2.decompress(source.read()))
                logger.info("Extraction completed successfully.")
            except Exception as e:
                logger.error(f"Failed to extract file: {e}")
                return

        logger.info("Download and extraction completed successfully.")

    def export_to_csv(self):
        """Export tables from SQLite database to CSV files."""
        if not self.db_file.exists():
            logger.error(f"SQLite database not found at {self.db_file}. Run with --download first.")
            return

        logger.info(f"Exporting SQLite tables to CSV from {self.db_file}...")

        try:
            # Connect to the SQLite database
            conn = sqlite3.connect(str(self.db_file))
            cursor = conn.cursor()

            # Get a list of all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]

            for table in tables:
                csv_file = self.csv_dir / f"{table}.csv"

                logger.info(f"Exporting table {table} to {csv_file}...")

                # Get column names
                cursor.execute(f"PRAGMA table_info({table});")
                columns = [row[1] for row in cursor.fetchall()]

                # Get all rows from the table
                cursor.execute(f"SELECT * FROM {table};")
                rows = cursor.fetchall()

                # Write to CSV
                with open(csv_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)  # Write header
                    writer.writerows(rows)  # Write data

                logger.info(f"Exported {len(rows)} rows from {table} to {csv_file}")

            conn.close()
            logger.info("CSV export completed successfully.")
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")

    def export(self):
        """
        Export WOF data from SQLite database to CSV files.

        This method fulfills the BaseDownloader abstract method requirement
        by calling the existing export_to_csv method.
        """
        return self.export_to_csv()


def main():
    """Parse command line arguments and run the downloader."""
    parser = argparse.ArgumentParser(description="Who's on First Gazetteer Data Downloader")
    parser.add_argument(
        "--download", action="store_true", help="Download and extract the SQLite database"
    )
    parser.add_argument("--export", action="store_true", help="Export SQLite tables to CSV files")
    parser.add_argument("--all", action="store_true", help="Perform all operations in sequence")
    parser.add_argument("--data-dir", help="Custom data directory path")

    args = parser.parse_args()

    # If no arguments are provided, show help
    if not (args.download or args.export or args.all):
        parser.print_help()
        return

    # Create and run the downloader
    downloader = WofDownloader(data_dir=args.data_dir)
    downloader.run(download=args.download, export=args.export, all=args.all)


if __name__ == "__main__":
    main()
