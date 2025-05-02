import argparse
import logging
from app.gazetteer.downloaders import GeonamesDownloader, WofDownloader, FastDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

def download_gazetteer(gazetteer_name):
    """
    Download data for the specified gazetteer.

    Args:
        gazetteer_name (str): Name of the gazetteer to download.

    Returns:
        bool: True if download was successful, False otherwise.
    """
    try:
        if gazetteer_name.lower() == "geonames":
            downloader = GeonamesDownloader()
        elif gazetteer_name.lower() == "wof":
            downloader = WofDownloader()
        elif gazetteer_name.lower() == "fast":
            downloader = FastDownloader()
        else:
            logger.error(f"Unknown gazetteer: {gazetteer_name}")
            return False

        result = downloader.download()
        if result["status"] == "success":
            logger.info(f"Successfully downloaded {gazetteer_name} data")
            return True
        else:
            logger.error(f"Failed to download {gazetteer_name} data: {result.get('error', 'Unknown error')}")
            return False

    except Exception as e:
        logger.error(f"Error downloading {gazetteer_name} data: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Download gazetteer data")
    parser.add_argument(
        "--gazetteer",
        choices=["geonames", "wof", "fast"],
        required=True,
        help="Gazetteer to download (geonames, wof, or fast)",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export the downloaded data to CSV format",
    )

    args = parser.parse_args()

    if download_gazetteer(args.gazetteer):
        if args.export:
            if args.gazetteer == "geonames":
                downloader = GeonamesDownloader()
            elif args.gazetteer == "wof":
                downloader = WofDownloader()
            elif args.gazetteer == "fast":
                downloader = FastDownloader()
            
            result = downloader.export()
            if result["status"] == "success":
                logger.info(f"Successfully exported {args.gazetteer} data")
            else:
                logger.error(f"Failed to export {args.gazetteer} data: {result.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 