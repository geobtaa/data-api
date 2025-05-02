import asyncio
import json
import logging
from typing import Any, Dict, Optional, Tuple

import aiohttp

# Setup logging
logger = logging.getLogger(__name__)


class SummaryGenerator:
    def __init__(self, api_key: str, model: str, api_url: str):
        self.api_key = api_key
        self.model = model
        self.api_url = api_url

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

        # Log the prompt and configuration
        logger.info(f"Generating summary with model {self.model}")
        logger.debug(f"Summary prompt: {prompt}")
        logger.debug(f"Output parser configuration: {output_parser}")

        # Call OpenAI API with timeout
        timeout = aiohttp.ClientTimeout(total=60)  # 1 minute timeout
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}

        # Log the API request configuration
        logger.debug(f"API URL: {self.api_url}")
        logger.debug(f"Request timeout: {timeout.total} seconds")

        try:
            logger.info("Making API request to OpenAI")
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    self.api_url,
                    headers=headers,
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.7,
                    },
                ) as response:
                    logger.debug(f"API Response status: {response.status}")
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(
                            f"API request failed with status {response.status}: {error_text}"
                        )
                        raise Exception(f"OpenAI API request failed: {error_text}")

                    response_data = await response.json()
                    logger.debug("Successfully received API response")

                    # Extract the summary from the response
                    summary = response_data["choices"][0]["message"]["content"].strip()
                    logger.info(f"Generated summary of length {len(summary)}")
                    logger.debug(f"Summary content: {summary}")

                    return summary, prompt, output_parser

        except asyncio.TimeoutError:
            logger.error("API request timed out after 60 seconds")
            raise
        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            raise

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
Summarize these key points, but do not make a numbered list in your response.
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
