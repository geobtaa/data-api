import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

# Setup logging
logger = logging.getLogger(__name__)


class GeoEntityIdentifier:
    def __init__(self, api_key: str, model: str, api_url: str, gazetteer_service=None):
        self.api_key = api_key
        self.model = model
        self.api_url = api_url
        self.gazetteer_service = gazetteer_service
        
        logger.info("Initialized GeoEntityIdentifier with local gazetteer service")

    async def identify_geo_entities(
        self, text: str, context: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Identify geographic named entities in text and map them to local gazetteer entries.

        Args:
            text: Text content to analyze for geographic entities
            context: Optional dictionary containing additional context for entity identification

        Returns:
            Tuple containing:
            - List[Dict]: List of identified geographic entities with gazetteer mappings
            - Dict: The prompt used for generation
            - Dict: The output parser configuration
        """
        # Construct the prompt
        prompt, output_parser = self._construct_geo_entity_prompt(text, context)

        # Log the prompt and configuration
        logger.info(f"Identifying geographic entities with model {self.model}")
        logger.debug(f"Entity identification prompt: {prompt}")
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
                        "temperature": 0.3,  # Lower temperature for more accurate entity identification
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

                    # Extract the entity identification results from the response
                    entity_results = json.loads(response_data["choices"][0]["message"]["content"])
                    logger.info(f"Identified {len(entity_results)} geographic entities")
                    logger.debug(f"Entity identification results: {entity_results}")

                    # Enrich entities with local gazetteer data
                    enriched_entities = await self._enrich_with_gazetteer(entity_results)
                    logger.info(f"Enriched {len(enriched_entities)} entities with gazetteer data")

                    return enriched_entities, prompt, output_parser

        except asyncio.TimeoutError:
            logger.error("API request timed out after 60 seconds")
            raise
        except Exception as e:
            logger.error(f"Error identifying geographic entities: {str(e)}")
            raise

    def _construct_geo_entity_prompt(
        self, text: str, context: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Construct a prompt for the LLM to identify geographic entities.

        Returns:
            Tuple containing:
            - str: The prompt text
            - Dict: The output parser configuration
        """
        prompt = """Identify all geographic named entities in the following text.

Text:
{text}

"""
        if context:
            prompt += f"""
Additional Context:
{json.dumps(context, indent=2)}

"""

        prompt += """
For each geographic entity, provide:
1. The entity name as it appears in the text
2. The type of entity (e.g., city, country, river, mountain)
3. Any additional context that might help with disambiguation

Format your response as a JSON array of objects with the following structure:
[
  {{
    "name": "entity name",
    "type": "entity type",
    "context": "additional context"
  }}
]"""

        # Define the output parser configuration
        output_parser = {
            "type": "json",
            "description": "A list of identified geographic entities with their types and context",
        }

        return prompt.format(text=text), output_parser

    async def _enrich_with_gazetteer(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich identified entities with local gazetteer data.

        Args:
            entities: List of identified geographic entities

        Returns:
            List of entities enriched with gazetteer data
        """
        enriched_entities = []

        if not self.gazetteer_service:
            logger.warning("No gazetteer service provided. Returning entities without enrichment.")
            return entities

        for entity in entities:
            try:
                # Use the local gazetteer service to look up the entity
                gazetteer_entry = await self.gazetteer_service.lookup_place(
                    name=entity["name"],
                    entity_type=entity["type"],
                    context=entity.get("context", ""),
                )

                if gazetteer_entry:
                    # Add gazetteer data to the entity
                    entity["gazetteer_id"] = gazetteer_entry.get("id")
                    entity["gazetteer_name"] = gazetteer_entry.get("name")
                    entity["gazetteer_type"] = gazetteer_entry.get("type")
                    entity["gazetteer_country"] = gazetteer_entry.get("country")
                    entity["gazetteer_lat"] = gazetteer_entry.get("latitude")
                    entity["gazetteer_lng"] = gazetteer_entry.get("longitude")
                    entity["gazetteer_confidence"] = gazetteer_entry.get("confidence", 0.0)
                else:
                    logger.warning(f"No gazetteer match found for entity: {entity['name']}")
            except Exception as e:
                logger.error(
                    f"Error enriching entity {entity['name']} with gazetteer data: {str(e)}"
                )

            enriched_entities.append(entity)

        return enriched_entities
