import logging
import os
import tempfile
import zipfile
from urllib.request import urlretrieve

from .base_downloader import BaseDownloader

logger = logging.getLogger(__name__)


class FastDownloader(BaseDownloader):
    """Downloader for OCLC FAST Dataset Geographic entries."""

    # FAST-specific data directory
    DATA_DIR = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "data",
        "gazetteers",
        "fast",
    )

    # URL for the FAST Geographic MARCXML dataset
    FAST_URL = "https://researchworks.oclc.org/researchdata/fast/FASTGeographic.marcxml.zip"

    def __init__(self, data_dir=None):
        """Initialize the FAST downloader."""
        super().__init__(data_dir=data_dir, gazetteer_name="fast")
        self.marcxml_file = None

    def download(self):
        """
        Download and extract the FAST MARCXML dataset.

        Returns:
            Dictionary with download results.
        """
        try:
            # Create data directory if it doesn't exist
            os.makedirs(self.data_dir, exist_ok=True)

            # Create a temporary directory for extraction
            with tempfile.TemporaryDirectory() as temp_dir:
                zip_path = os.path.join(temp_dir, "FASTGeographic.marcxml.zip")

                # Download the file
                logger.info(f"Downloading FAST dataset from {self.FAST_URL}")
                urlretrieve(self.FAST_URL, zip_path)
                logger.info(f"Downloaded file to {zip_path}")

                # Check if the file was downloaded successfully
                if not os.path.exists(zip_path):
                    raise FileNotFoundError(f"Failed to download file from {self.FAST_URL}")

                file_size = os.path.getsize(zip_path)
                logger.info(f"Downloaded file size: {file_size} bytes")

                if file_size == 0:
                    raise ValueError("Downloaded file is empty")

                # Extract the file
                logger.info("Extracting MARCXML file")
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    # List all files in the zip
                    all_files = zip_ref.namelist()
                    logger.info(f"Files in ZIP: {all_files}")

                    # Find the MARCXML file in the zip
                    marcxml_files = [f for f in all_files if f.endswith(".marcxml")]
                    if not marcxml_files:
                        raise FileNotFoundError("No MARCXML file found in the ZIP archive")

                    # Extract the first MARCXML file
                    marcxml_filename = marcxml_files[0]
                    logger.info(f"Extracting {marcxml_filename} from ZIP")
                    zip_ref.extract(marcxml_filename, temp_dir)

                    # Move the extracted file to the data directory
                    extracted_path = os.path.join(temp_dir, marcxml_filename)
                    target_path = os.path.join(self.data_dir, "FASTGeographic.marcxml")

                    # Copy the file to the data directory
                    with open(extracted_path, "rb") as src, open(target_path, "wb") as dst:
                        dst.write(src.read())

                    # Check if the file was copied successfully
                    if not os.path.exists(target_path):
                        raise FileNotFoundError(f"Failed to copy file to {target_path}")

                    file_size = os.path.getsize(target_path)
                    logger.info(f"Extracted MARCXML file size: {file_size} bytes")

                    if file_size == 0:
                        raise ValueError("Extracted MARCXML file is empty")

                    self.marcxml_file = target_path
                    logger.info(f"Extracted MARCXML file to {target_path}")

            return {
                "status": "success",
                "message": "FAST data downloaded and extracted successfully",
                "file": self.marcxml_file,
            }

        except Exception as e:
            logger.error(f"Error downloading FAST data: {e}")
            return {"status": "error", "error": str(e)}

    def export(self):
        """
        Export is not needed for FAST data as it's already in MARCXML format.
        The conversion to CSV is handled by the importer.

        Returns:
            Dictionary indicating success.
        """
        return {"status": "success", "message": "No export needed for FAST data"}
