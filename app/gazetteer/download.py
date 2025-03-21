#!/usr/bin/env python3
"""
Gazetteer Data Downloader

This script provides a command-line interface for downloading gazetteer data.
It can be used to download data for one or more gazetteers.

Usage:
    python -m app.gazetteer.download [options]

Arguments:
    --gazetteer     Gazetteer to download (wof, btaa, geonames). Can be specified multiple times.
    --download      Download and extract data.
    --export        Export data to CSV (for gazetteers that need this step).
    --all           Run all operations for the specified gazetteer(s).
    --all-gazetteers Download all available gazetteers.
    --data-dir      Custom data directory path.
"""

import argparse
import logging
import sys
from datetime import datetime

# Use absolute imports instead of relative
from app.gazetteer.downloaders import WofDownloader, GeoNamesDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("gazetteer_download")

# Map of available downloaders
DOWNLOADERS = {
    'wof': WofDownloader,
    'geonames': GeoNamesDownloader,
}

def download_gazetteer(gazetteer_name, download=False, export=False, all_ops=False, data_dir=None):
    """
    Download data for a specific gazetteer.
    
    Args:
        gazetteer_name: Name of the gazetteer.
        download: Whether to download data.
        export: Whether to export data.
        all_ops: Whether to perform all operations.
        data_dir: Optional data directory path.
        
    Returns:
        Dictionary with download results.
    """
    if gazetteer_name not in DOWNLOADERS:
        logger.error(f"Unsupported gazetteer: {gazetteer_name}")
        return {
            "status": "error",
            "gazetteer": gazetteer_name,
            "error": f"Unsupported gazetteer: {gazetteer_name}"
        }
    
    downloader_class = DOWNLOADERS[gazetteer_name]
    downloader = downloader_class(data_dir=data_dir)
    
    logger.info(f"Starting download for {gazetteer_name} gazetteer")
    result = downloader.run(
        download=download,
        export=export,
        all=all_ops
    )
    
    return result

def main():
    """Parse command line arguments and run the downloader."""
    parser = argparse.ArgumentParser(description="Gazetteer Data Downloader")
    parser.add_argument('--gazetteer', action='append', choices=list(DOWNLOADERS.keys()), 
                        help='Gazetteer to download. Can be specified multiple times.')
    parser.add_argument('--download', action='store_true', help='Download gazetteer data')
    parser.add_argument('--export', action='store_true', help='Export data to CSV (for gazetteers that support this)')
    parser.add_argument('--all', action='store_true', help='Perform all operations for the specified gazetteer(s)')
    parser.add_argument('--all-gazetteers', action='store_true', help='Download all available gazetteers')
    parser.add_argument('--data-dir', help='Custom data directory path')
    
    args = parser.parse_args()
    
    # If both --all and specific options are provided, --all takes precedence
    if args.all:
        args.download = True
        args.export = True
    
    # If no gazetteers are specified, but --all-gazetteers is set, use all
    if args.all_gazetteers:
        gazetteers = list(DOWNLOADERS.keys())
    # Otherwise, use the specified gazetteers
    elif args.gazetteer:
        gazetteers = args.gazetteer
    # If no gazetteers are specified, show help
    else:
        parser.print_help()
        return
    
    # If no operations are specified, show help
    if not (args.download or args.export or args.all):
        parser.print_help()
        return
    
    start_time = datetime.now()
    results = {}
    
    # Download each gazetteer
    for gazetteer in gazetteers:
        results[gazetteer] = download_gazetteer(
            gazetteer,
            download=args.download,
            export=args.export,
            all_ops=args.all,
            data_dir=args.data_dir
        )
    
    # Print summary
    elapsed_time = (datetime.now() - start_time).total_seconds()
    logger.info(f"All downloads completed in {elapsed_time:.2f} seconds")
    
    for gazetteer, result in results.items():
        status = result.get('status', 'unknown')
        if status == 'success':
            logger.info(f"{gazetteer}: Success in {result.get('elapsed_time', 0):.2f} seconds")
        else:
            logger.error(f"{gazetteer}: {status.capitalize()} - {result.get('error', 'Unknown error')}")
    
    # Return non-zero exit code if any download failed
    for result in results.values():
        if result.get('status') != 'success':
            sys.exit(1)

if __name__ == "__main__":
    main() 