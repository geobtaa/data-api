import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import openai
from dotenv import load_dotenv

from app.services.llm import GeoEntityIdentifier, SummaryGenerator

# Load environment variables from .env file
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all logs

# Get log path from environment variable or use default
log_path = os.getenv("LOG_PATH", "logs")
log_file = os.path.join(log_path, "llm_service.log")

# Ensure log directory exists with proper permissions
Path(log_path).mkdir(parents=True, exist_ok=True)

# Create formatter
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

# Add file handler for LLM service logs
llm_log_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
llm_log_handler.setFormatter(formatter)
llm_log_handler.setLevel(logging.DEBUG)

# Add stream handler for console output
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
console_handler.setLevel(logging.DEBUG)

# Remove any existing handlers and ensure logger doesn't propagate to root
logger.handlers = []
logger.propagate = False

# Add handlers to logger
logger.addHandler(llm_log_handler)
logger.addHandler(console_handler)

# Add a test log message to verify logging is working
logger.info("LLM Service logger initialized")
logger.debug("LLM Service debug logging enabled")


class LLMService:
    """Service for interacting with OpenAI's LLM models."""

    def __init__(self, api_key: str = None):
        """Initialize the LLM service with OpenAI client."""
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY environment variable is not set")

        self.model = os.getenv("OPENAI_MODEL", "gpt-4-vision-preview")
        self.api_url = "https://api.openai.com/v1/chat/completions"

        # Configure OpenAI
        openai.api_key = self.api_key
        openai.api_base = self.api_url

        # Initialize the geo entity identifier
        self.geo_entity_identifier = GeoEntityIdentifier(
            api_key=self.api_key, model=self.model, api_url=self.api_url
        )

        # Initialize the summary generator
        self.summary_generator = SummaryGenerator(
            api_key=self.api_key, model=self.model, api_url=self.api_url
        )

    async def identify_geo_entities(self, text: str) -> List[Dict]:
        """
        Identify geographic entities in text using OpenAI's LLM.

        Args:
            text: The text to analyze

        Returns:
            List of dictionaries containing geographic entities and their metadata
        """
        try:
            # Use the geo entity identifier
            return await self.geo_entity_identifier.identify_geo_entities(text)
        except Exception as e:
            logger.error(f"Error identifying geographic entities: {str(e)}")
            raise

    async def perform_ocr(self, image_data: bytes) -> str:
        """
        Perform OCR on an image using OpenAI's vision model.

        Args:
            image_data: The image data in bytes

        Returns:
            Extracted text from the image
        """
        try:
            # Convert image data to base64
            import base64

            image_base64 = base64.b64encode(image_data).decode("utf-8")

            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Extract all text from this image. Include any dates, numbers, and special characters. Preserve the original formatting and line breaks.",
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"},
                            },
                        ],
                    }
                ],
                max_tokens=1000,
            )

            ocr_text = response.choices[0].message.content
            logger.info("Successfully extracted text from image")
            return ocr_text

        except Exception as e:
            logger.error(f"Error performing OCR: {str(e)}")
            raise

    async def generate_summary(
        self, metadata: Dict[str, Any], asset_content: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        """
        Generate a summary of the metadata and asset content using OpenAI's ChatGPT API.

        Args:
            metadata: Dictionary containing the item's metadata
            asset_content: Optional string containing the processed content of the asset
                          (e.g., OCR text for images, attribute descriptions for shapefiles)

        Returns:
            Tuple containing:
            - str: Generated summary of the item
            - Dict: The prompt used for generation
            - Dict: The output parser configuration
        """
        return await self.summary_generator.generate_summary(metadata, asset_content)

    async def process_asset(self, asset_path: str, asset_type: str) -> Optional[str]:
        """
        Process different types of assets to extract text content for summarization.

        Args:
            asset_path: Path to the asset file or URL
            asset_type: Type of the asset (e.g., 'iiif_image', 'iiif_manifest', 'cog',
                        'pmtiles', 'download')

        Returns:
            Optional[str]: Extracted text content from the asset
        """
        if not asset_path:
            return None

        logger.info(f"Processing asset of type {asset_type} at {asset_path}")

        try:
            if asset_type == "iiif_image":
                return await self._process_iiif_image(asset_path)
            elif asset_type == "iiif_manifest":
                return await self._process_iiif_manifest(asset_path)
            elif asset_type == "cog":
                return await self._process_cog(asset_path)
            elif asset_type == "pmtiles":
                return await self._process_pmtiles(asset_path)
            elif asset_type == "download":
                # For download URLs, determine the file type from the extension or content type
                return await self._process_download(asset_path)
            else:
                # For unknown types, try to determine based on file extension
                return await self._process_download(asset_path)
        except Exception as e:
            logger.error(f"Error processing asset {asset_path} of type {asset_type}: {str(e)}")
            return None

    async def _process_iiif_image(self, image_url: str) -> Optional[str]:
        """Process a IIIF image URL to extract metadata and visual content."""
        # TODO: Implement IIIF image processing
        # This would involve:
        # 1. Fetching the IIIF image info.json
        # 2. Extracting metadata
        # 3. Potentially using OCR on the image
        return f"IIIF Image: {image_url}"

    async def _process_iiif_manifest(self, manifest_url: str) -> Optional[str]:
        """Process a IIIF manifest to extract metadata and content."""
        # TODO: Implement IIIF manifest processing
        # This would involve:
        # 1. Fetching the manifest JSON
        # 2. Extracting metadata, labels, descriptions
        # 3. Finding image resources within the manifest
        return f"IIIF Manifest: {manifest_url}"

    async def _process_cog(self, cog_url: str) -> Optional[str]:
        """Process a Cloud Optimized GeoTIFF to extract metadata."""
        # TODO: Implement COG processing
        # This would involve:
        # 1. Reading COG metadata
        # 2. Extracting geospatial information
        return f"Cloud Optimized GeoTIFF: {cog_url}"

    async def _process_pmtiles(self, pmtiles_url: str) -> Optional[str]:
        """Process a PMTiles asset to extract metadata."""
        # TODO: Implement PMTiles processing
        # This would involve:
        # 1. Reading PMTiles metadata
        # 2. Extracting tile information
        return f"PMTiles: {pmtiles_url}"

    async def _process_download(self, download_url: str) -> Optional[str]:
        """Process a download URL to determine file type and extract content."""
        # TODO: Implement download processing
        # This would involve:
        # 1. Determining file type from URL or content type
        # 2. Processing based on file type (shapefile, geodatabase, etc.)
        return f"Download URL: {download_url}"

    async def generate_ocr(
        self, metadata: Dict[str, Any], asset_content: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any], Dict[str, Any]]:
        """
        Generate OCR text from the image using OpenAI's ChatGPT API.

        Args:
            metadata: Dictionary containing the item's metadata
            asset_content: Optional string containing the processed content of the asset
                          (e.g., image data for OCR)

        Returns:
            Tuple containing:
            - str: Generated OCR text
            - Dict: The prompt used for generation
            - Dict: The output parser configuration
        """
        # Construct the prompt
        prompt, output_parser = self._construct_ocr_prompt(metadata, asset_content)

        # Log the prompt and configuration
        logger.info(f"Generating OCR text with model {self.model}")
        logger.debug(f"OCR prompt: {prompt}")
        logger.debug(f"Output parser configuration: {output_parser}")

        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are a helpful assistant that extracts text from "
                        "historical maps and geographic datasets using OCR.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.3,  # Lower temperature for more accurate OCR
                max_tokens=1000,  # Higher token limit for OCR text
                top_p=0.8,
            )

            ocr_text = response.choices[0].message.content
            logger.info(f"Successfully generated OCR text of length {len(ocr_text)}")
            logger.debug(f"Generated OCR text: {ocr_text}")
            return ocr_text, prompt, output_parser

        except Exception as e:
            logger.error(f"Error generating OCR text with OpenAI API: {str(e)}")
            raise Exception(f"Error generating OCR text with OpenAI API: {str(e)}") from e

    def _construct_ocr_prompt(
        self, metadata: Dict[str, Any], asset_content: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Construct a prompt for the LLM to generate OCR text.

        Returns:
            Tuple containing:
            - str: The prompt text
            - Dict: The output parser configuration
        """
        prompt = """Extract all text from this historical map or dataset using OCR.

Metadata:
{metadata}

"""
        if asset_content:
            prompt += f"""
Content:
{asset_content}

"""

        prompt += """
Please extract all text visible in the image, including:
1. Titles and headings
2. Labels and annotations
3. Legend text
4. Scale information
5. Any other text visible in the image

Format the output as plain text, preserving the relative positions of text elements where possible.
Include any numerical values, dates, or measurements exactly as they appear."""

        # Define the output parser configuration
        output_parser = {
            "type": "text",
            "description": "OCR text extracted from the historical map or geographic dataset",
        }

        return prompt.format(metadata=json.dumps(metadata, indent=2)), output_parser
