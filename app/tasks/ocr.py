from typing import Optional, Dict, Any
from celery import shared_task
import logging
import json
from datetime import datetime
from db.database import database
from db.models import ai_enrichments
from sqlalchemy import insert
import asyncio
import time
import os
import requests
import pytesseract
from PIL import Image
import io

logger = logging.getLogger(__name__)


# Custom JSON encoder to handle datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


@shared_task(
    soft_time_limit=180,  # 3 minutes
    time_limit=240,  # 4 minutes
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 5},
    retry_backoff=True,
)
def generate_item_ocr(
    item_id: str,
    metadata: Dict[str, Any],
    asset_path: Optional[str] = None,
    asset_type: Optional[str] = None,
) -> Dict:
    """Generate OCR text for a document."""
    logger.info(f"Starting OCR generation for item {item_id}")
    logger.info(f"Asset path: {asset_path}, Asset type: {asset_type}")

    try:
        # Set up event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            # Generate OCR text
            ocr_text = loop.run_until_complete(
                _generate_ocr(item_id, metadata, asset_path, asset_type)
            )

            if not ocr_text:
                logger.warning(f"No OCR text was generated for item {item_id}")
                return {
                    "status": "error",
                    "message": "No OCR text was generated",
                    "item_id": item_id,
                }

            # Store OCR text in database
            try:
                store_ocr_in_db(item_id, ocr_text)
                logger.info(f"Successfully stored OCR text for item {item_id}")
                return {
                    "status": "success",
                    "message": "OCR text generated and stored successfully",
                    "item_id": item_id,
                }
            except Exception as e:
                logger.error(f"Error storing OCR text in database for item {item_id}: {str(e)}")
                logger.exception("Full traceback:")
                return {
                    "status": "error",
                    "message": f"Error storing OCR text: {str(e)}",
                    "item_id": item_id,
                }

        finally:
            loop.close()

    except Exception as e:
        logger.error(f"Error generating OCR for item {item_id}: {str(e)}")
        logger.exception("Full traceback:")
        return {"status": "error", "message": f"Error generating OCR: {str(e)}", "item_id": item_id}


async def _generate_ocr(
    item_id: str,
    metadata: Dict[str, Any],
    asset_path: Optional[str] = None,
    asset_type: Optional[str] = None,
) -> str:
    """Async helper function to generate OCR text."""
    try:
        # Ensure database is connected
        if not database.is_connected:
            await database.connect()

        logger.info(f"Starting OCR generation for item {item_id}")

        # Process the asset if provided
        ocr_text = None
        if asset_path and asset_type:
            logger.info(f"Processing asset {asset_path} of type {asset_type}")
            ocr_text = await process_asset_for_ocr(asset_path, asset_type)
            logger.info(f"OCR text generated: {bool(ocr_text)}")

        if not ocr_text:
            logger.warning(f"No OCR text generated for item {item_id}")
            return ""

        # Store the OCR text in the database
        logger.info("Storing OCR text in database")
        await store_ocr_in_db(
            document_id=item_id,
            ocr_text=ocr_text,
        )
        logger.info("OCR text stored in database")

        return ocr_text

    except Exception as e:
        logger.error(f"Error in _generate_ocr for item {item_id}: {str(e)}")
        logger.exception("Full traceback:")
        raise
    finally:
        # Clean up database connection
        if database.is_connected:
            await database.disconnect()


async def process_asset_for_ocr(asset_path: str, asset_type: str) -> Optional[str]:
    """
    Process different types of assets to extract text using OCR.

    Args:
        asset_path: Path to the asset file or URL
        asset_type: Type of the asset (e.g., 'iiif_image', 'iiif_manifest', 'download')

    Returns:
        Optional[str]: Extracted OCR text from the asset
    """
    if not asset_path:
        return None

    logger.info(f"Processing asset of type {asset_type} at {asset_path}")

    try:
        if asset_type == "iiif_image":
            return await _process_iiif_image_ocr(asset_path)
        elif asset_type == "iiif_manifest":
            return await _process_iiif_manifest_ocr(asset_path)
        elif asset_type == "download":
            return await _process_download_ocr(asset_path)
        else:
            # For unknown types, try to determine based on file extension
            return await _process_download_ocr(asset_path)
    except Exception as e:
        logger.error(f"Error processing asset {asset_path} of type {asset_type}: {str(e)}")
        return None


async def _process_iiif_image_ocr(image_url: str) -> Optional[str]:
    """Process a IIIF image URL to extract text using OCR."""
    try:
        # Remove /info.json from URL if present
        base_url = image_url.replace("/info.json", "")

        # Get a full-size image for better OCR results
        image_url = f"{base_url}/full/full/0/default.jpg"

        # Download the image
        response = requests.get(image_url, timeout=30)
        response.raise_for_status()

        # Convert to PIL Image
        image = Image.open(io.BytesIO(response.content))

        # Perform OCR
        ocr_text = pytesseract.image_to_string(image)

        return ocr_text.strip()
    except Exception as e:
        logger.error(f"Error processing IIIF image OCR {image_url}: {str(e)}")
        return None


async def _process_iiif_manifest_ocr(manifest_url: str) -> Optional[str]:
    """Process a IIIF manifest to extract text from its images using OCR."""
    try:
        # Get the manifest
        logger.info(f"Fetching IIIF manifest from {manifest_url}")
        response = requests.get(manifest_url, timeout=30)
        response.raise_for_status()
        manifest = response.json()

        # Log manifest structure for debugging
        logger.info(f"Manifest structure: {json.dumps(manifest, indent=2)}")

        # Extract image URLs from the manifest
        image_urls = []

        # Check sequences (IIIF 2.0)
        if manifest.get("sequences"):
            logger.info("Processing IIIF 2.0 manifest")
            for sequence in manifest["sequences"]:
                for canvas in sequence.get("canvases", []):
                    for image in canvas.get("images", []):
                        if resource := image.get("resource", {}):
                            if image_id := resource.get("@id"):
                                # Get the IIIF image service URL
                                if service := resource.get("service", {}):
                                    if service_id := service.get("@id"):
                                        # Construct full-size image URL
                                        image_url = f"{service_id}/full/full/0/default.jpg"
                                        logger.info(f"Found IIIF 2.0 image URL: {image_url}")
                                        image_urls.append(image_url)
                                    else:
                                        logger.warning(f"No service ID found for image {image_id}")
                                        image_urls.append(image_id)
                                else:
                                    logger.warning(f"No service found for image {image_id}")
                                    image_urls.append(image_id)

        # Check items (IIIF 3.0)
        elif manifest.get("items"):
            logger.info("Processing IIIF 3.0 manifest")
            for item in manifest["items"]:
                for canvas in item.get("items", []):
                    for annotation in canvas.get("items", []):
                        if body := annotation.get("body", {}):
                            if image_id := body.get("id"):
                                # Get the IIIF image service URL
                                if service := body.get("service", []):
                                    if service_id := service[0].get("id"):
                                        # Construct full-size image URL
                                        image_url = f"{service_id}/full/full/0/default.jpg"
                                        logger.info(f"Found IIIF 3.0 image URL: {image_url}")
                                        image_urls.append(image_url)
                                    else:
                                        logger.warning(f"No service ID found for image {image_id}")
                                        image_urls.append(image_id)
                                else:
                                    logger.warning(f"No service found for image {image_id}")
                                    image_urls.append(image_id)

        if not image_urls:
            logger.warning(f"No images found in IIIF manifest {manifest_url}")
            return None

        # Process each image
        ocr_texts = []
        for image_url in image_urls:
            try:
                logger.info(f"Downloading image from {image_url}")
                # Download the image
                image_response = requests.get(image_url, timeout=30)
                image_response.raise_for_status()

                # Convert to PIL Image
                image = Image.open(io.BytesIO(image_response.content))

                # Perform OCR
                logger.info(f"Performing OCR on image {image_url}")
                ocr_text = pytesseract.image_to_string(image)
                if ocr_text.strip():
                    ocr_texts.append(ocr_text.strip())
                    logger.info(f"Successfully extracted OCR text from image {image_url}")
                else:
                    logger.warning(f"No OCR text extracted from image {image_url}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Error downloading image {image_url}: {str(e)}")
                continue
            except Exception as e:
                logger.error(f"Error processing image {image_url}: {str(e)}")
                logger.exception("Full traceback:")
                continue

        if not ocr_texts:
            logger.warning("No OCR text was extracted from any images in the manifest")
            return None

        return "\n\n".join(ocr_texts)
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching IIIF manifest {manifest_url}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing IIIF manifest {manifest_url}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error processing IIIF manifest OCR {manifest_url}: {str(e)}")
        logger.exception("Full traceback:")
        raise


async def _process_download_ocr(download_url: str) -> Optional[str]:
    """Process a download URL to extract text using OCR."""
    try:
        # Download the file
        response = requests.get(download_url, timeout=30)
        response.raise_for_status()

        # Check if it's an image
        content_type = response.headers.get("content-type", "").lower()
        if not content_type.startswith("image/"):
            logger.warning(
                f"Download URL {download_url} is not an image (content-type: {content_type})"
            )
            return None

        # Convert to PIL Image
        image = Image.open(io.BytesIO(response.content))

        # Perform OCR
        ocr_text = pytesseract.image_to_string(image)

        return ocr_text.strip()
    except Exception as e:
        logger.error(f"Error processing download OCR {download_url}: {str(e)}")
        return None


async def store_ocr_in_db(
    document_id: str,
    ocr_text: str,
):
    """
    Store the generated OCR text in the ai_enrichments table.

    Args:
        document_id: The ID of the document
        ocr_text: The generated OCR text
    """
    try:
        # Prepare the data for insertion
        now = datetime.utcnow()

        # Create a structured response object
        response_data = {"ocr_text": ocr_text, "timestamp": now.isoformat()}

        # Create the enrichment record
        enrichment_data = {
            "document_id": document_id,
            "ai_provider": "Tesseract",
            "model": "tesseract-ocr",
            "enrichment_type": "ocr",
            "response": response_data,
            "created_at": now,
            "updated_at": now,
        }

        # Insert the record into the database
        async with database.transaction():
            query = insert(ai_enrichments).values(**enrichment_data)
            await database.execute(query)

        logger.info(f"Stored OCR text for document {document_id} in the database")

    except Exception as e:
        logger.error(f"Error storing OCR text in database: {str(e)}")
        raise
