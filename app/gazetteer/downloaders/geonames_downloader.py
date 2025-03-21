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

import os
import sys
import zipfile
import requests
import logging
import argparse
from pathlib import Path
from datetime import datetime

from .base_downloader import BaseDownloader

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
        """Download the GeoNames data and extract it."""
        if self.txt_file.exists():
            logger.info(f"GeoNames data file already exists at {self.txt_file}")
            return
        
        if not self.zip_file.exists():
            logger.info(f"Downloading file from {self.GEONAMES_URL}...")
            try:
                response = requests.get(self.GEONAMES_URL, stream=True)
                response.raise_for_status()
                
                # Get total file size for progress reporting
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                chunk_size = 8192
                
                with open(self.zip_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = downloaded / total_size * 100 if total_size > 0 else 0
                            sys.stdout.write(f"\rDownloading... {progress:.1f}% ({downloaded/(1024*1024):.1f}MB / {total_size/(1024*1024):.1f}MB)")
                            sys.stdout.flush()
                
                print("\nDownload completed successfully.")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download file: {e}")
                return
        else:
            logger.info(f"ZIP file already exists at {self.zip_file}")
        
        # Extract the ZIP file
        logger.info(f"Extracting {self.zip_file}...")
        try:
            with zipfile.ZipFile(self.zip_file, 'r') as zip_ref:
                # Find the US.txt file in the archive
                for file_info in zip_ref.infolist():
                    if file_info.filename == 'US.txt':
                        # Extract the file
                        with zip_ref.open(file_info) as source, open(self.txt_file, 'wb') as dest:
                            dest.write(source.read())
                        logger.info(f"Extracted US.txt to {self.txt_file}")
                        break
                else:
                    logger.error("US.txt file not found in the ZIP archive")
                    return
            
            # Verify the extracted file exists
            if not self.txt_file.exists():
                logger.error(f"Failed to extract {self.txt_file} from the ZIP archive")
                return
            
            logger.info(f"Successfully extracted GeoNames data to {self.txt_file}")
            
            # Optionally remove the ZIP file to save space
            # self.zip_file.unlink()
            # logger.info(f"Removed ZIP file {self.zip_file}")
            
        except zipfile.BadZipFile:
            logger.error(f"The file {self.zip_file} is not a valid ZIP file")
            return
        except Exception as e:
            logger.error(f"Error extracting ZIP file: {e}")
            return
        
        logger.info("Download and extraction completed successfully.")

def main():
    """Parse command line arguments and run the downloader."""
    parser = argparse.ArgumentParser(description="GeoNames Gazetteer Data Downloader")
    parser.add_argument('--download', action='store_true', help='Download and extract GeoNames data')
    parser.add_argument('--all', action='store_true', help='Perform all operations in sequence')
    parser.add_argument('--data-dir', help='Custom data directory path')
    
    args = parser.parse_args()
    
    # If no arguments are provided, show help
    if not (args.download or args.all):
        parser.print_help()
        return
    
    # Create and run the downloader
    downloader = GeoNamesDownloader(data_dir=args.data_dir)
    result = downloader.run(
        download=args.download or args.all,
        all=args.all
    )
    
    # Print result
    if result.get('status') == 'success':
        logger.info(f"GeoNames download completed successfully in {result.get('elapsed_time', 0):.2f} seconds")
    else:
        logger.error(f"GeoNames download failed: {result.get('error', 'Unknown error')}")
        sys.exit(1)

if __name__ == "__main__":
    main() 