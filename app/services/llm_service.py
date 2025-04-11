import json
from typing import Optional, Dict, Any, Tuple
import aiohttp
from pydantic import BaseModel
import os
from dotenv import load_dotenv
import asyncio
import logging

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class LLMService:
    def __init__(self, api_key: Optional[str] = None, model: Optional[str] = None):
        # Use provided API key, environment variable, or raise error
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key is required. Set OPENAI_API_KEY environment variable or pass it to the constructor."
            )

        # Use model from constructor, environment variable, or default to gpt-3.5-turbo
        self.model = model or os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
        self.api_url = "https://api.openai.com/v1/chat/completions"

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
        # Construct the prompt
        prompt, output_parser = self._construct_summary_prompt(metadata, asset_content)

        # Call OpenAI API with timeout
        timeout = aiohttp.ClientTimeout(total=60)  # 1 minute timeout
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}

        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.post(
                    self.api_url,
                    headers=headers,
                    json={
                        "model": self.model,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a helpful assistant that provides clear, concise summaries of historical maps and geographic datasets.",
                            },
                            {"role": "user", "content": prompt},
                        ],
                        "temperature": 0.5,
                        "max_tokens": 500,
                        "top_p": 0.8,
                    },
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise Exception(f"Failed to generate summary: {error_text}")

                    result = await response.json()
                    summary = result["choices"][0]["message"]["content"]
                    return summary, prompt, output_parser
            except asyncio.TimeoutError:
                raise Exception("Timeout while generating summary with OpenAI API")
            except Exception as e:
                raise Exception(f"Error generating summary with OpenAI API: {str(e)}")

    def _construct_summary_prompt(
        self, metadata: Dict[str, Any], asset_content: Optional[str] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Construct a prompt for the LLM to generate a summary.

        Returns:
            Tuple containing:
            - str: The prompt text
            - Dict: The output parser configuration
        """
        prompt = """Generate a concise summary of this historical map or dataset.

Metadata:
{metadata}

"""
        if asset_content:
            prompt += f"""
Content:
{asset_content}

"""

        prompt += """
Summarize key points:
1. Main features and content
2. Historical context
3. Geographic coverage
4. Notable characteristics

Keep the summary focused and brief."""

        # Define the output parser configuration
        output_parser = {
            "type": "text",
            "description": "A concise summary of the historical map or geographic dataset",
        }

        return prompt.format(metadata=json.dumps(metadata, indent=2)), output_parser

    async def process_asset(self, asset_path: str, asset_type: str) -> Optional[str]:
        """
        Process different types of assets to extract text content for summarization.

        Args:
            asset_path: Path to the asset file
            asset_type: Type of the asset (e.g., 'image', 'shapefile', 'pdf')

        Returns:
            Optional[str]: Extracted text content from the asset
        """
        # TODO: Implement asset processing based on type
        # This would involve:
        # - For images: OCR processing
        # - For shapefiles: Extracting attribute descriptions
        # - For PDFs: Text extraction
        # For now, return None as this is a placeholder
        return None
