#!/usr/bin/env python3
"""
GeoNames Gazetteer Downloader

This module downloads GeoNames data for the US from the official website,
extracts the ZIP file, and prepares it for import by the GeoNamesImporter.

Features:
- Downloads GeoNames data from https://download.geonames.org/export/dump/US.zip
- Extracts the ZIP file to the appropriate directory
- Handles file naming and preparation for the importer

Usage:
    python -m app.gazetteer.downloaders.geonames_downloader [options]

Arguments:
    --download      Download and extract the GeoNames data
    --all           Perform all operations in sequence
"""

import argparse
import logging
import os
import sys
import zipfile

import requests

# Fix imports to work both as a module and as a direct script
try:
    # When run as a module
    from .base_downloader import BaseDownloader
except ImportError:
    # When run directly
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))
    from app.gazetteer.downloaders.base_downloader import BaseDownloader

# Setup logging
logger = logging.getLogger("geonames_downloader")


class GeoNamesDownloader(BaseDownloader):
    """Downloader for GeoNames gazetteer data."""

    # URL for downloading the GeoNames data
    GEONAMES_URL = "https://download.geonames.org/export/dump/US.zip"

    def __init__(self, data_dir=None):
        """
        Initialize the GeoNames downloader.

        Args:
            data_dir: Optional path to the data directory. If not provided, will use default.
        """
        super().__init__(data_dir=data_dir, gazetteer_name="geonames")

        # Zip and extracted file paths
        self.zip_file = self.data_dir / "US.zip"
        self.txt_file = self.data_dir / "US.txt"

    def download(self):
        """Download GeoNames data for the US."""
        url = "https://download.geonames.org/export/dump/US.zip"
        zip_path = self.data_dir / "US.zip"

        logger.info(f"Downloading GeoNames US data from {url}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(zip_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Download complete: {zip_path}")

        # Extract the ZIP file
        logger.info(f"Extracting {zip_path} to {self.data_dir}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(self.data_dir)

        logger.info("Extraction complete")

        # Remove .zip file after extraction
        zip_path.unlink()
        logger.info(f"Removed {zip_path}")

        return True

    def export(self):
        """
        Export GeoNames data.

        For GeoNames, the data is already in text format, so no additional export is needed.
        The .txt files will be directly consumed by the importer.
        """
        logger.info("GeoNames data is already in text format - no export needed.")
        return True


def main():
    """Parse command line arguments and run the downloader."""
    parser = argparse.ArgumentParser(description="GeoNames Gazetteer Data Downloader")
    parser.add_argument(
        "--download", action="store_true", help="Download and extract GeoNames data"
    )
    parser.add_argument("--all", action="store_true", help="Perform all operations in sequence")
    parser.add_argument("--data-dir", help="Custom data directory path")

    args = parser.parse_args()

    # If no arguments are provided, show help
    if not (args.download or args.all):
        parser.print_help()
        return

    # Create and run the downloader
    downloader = GeoNamesDownloader(data_dir=args.data_dir)
    result = downloader.run(download=args.download or args.all, all=args.all)

    # Print result
    if result.get("status") == "success":
        logger.info(
            f"GeoNames download completed successfully in {result.get('elapsed_time', 0):.2f} seconds"
        )
    else:
        logger.error(f"GeoNames download failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
