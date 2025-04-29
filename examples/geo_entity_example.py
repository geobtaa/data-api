#!/usr/bin/env python
"""
Example script demonstrating how to use the GazetteerService with the LLMService
to identify geographic entities in text.
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import the app modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.gazetteer_service import GazetteerService
from app.services.llm_service import LLMService
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

async def main():
    """
    Main function to demonstrate the geographic entity identification.
    """
    # Initialize the gazetteer service
    gazetteer_service = GazetteerService()
    
    # Initialize the LLM service with the gazetteer service
    llm_service = LLMService(gazetteer_service=gazetteer_service)
    
    # Example text with geographic entities
    text = """
    The Mississippi River flows through New Orleans, Louisiana, before emptying into the Gulf of Mexico.
    The city is known for its French Quarter and Mardi Gras celebrations.
    Nearby, you can visit the Great Smoky Mountains National Park, which spans parts of Tennessee and North Carolina.
    """
    
    # Identify geographic entities in the text
    logger.info("Identifying geographic entities in text...")
    entities, prompt, parser = await llm_service.identify_geo_entities(text)
    
    # Print the results
    logger.info(f"Identified {len(entities)} geographic entities:")
    for i, entity in enumerate(entities, 1):
        print(f"\nEntity {i}:")
        print(f"  Name: {entity.get('name')}")
        print(f"  Type: {entity.get('type')}")
        print(f"  Context: {entity.get('context')}")
        
        # Print gazetteer data if available
        if "gazetteer_id" in entity:
            print(f"  Gazetteer ID: {entity.get('gazetteer_id')}")
            print(f"  Gazetteer Name: {entity.get('gazetteer_name')}")
            print(f"  Gazetteer Type: {entity.get('gazetteer_type')}")
            print(f"  Country: {entity.get('gazetteer_country')}")
            print(f"  Coordinates: {entity.get('gazetteer_lat')}, {entity.get('gazetteer_lng')}")
            print(f"  Confidence: {entity.get('gazetteer_confidence', 0.0):.2f}")
        else:
            print("  No gazetteer match found")
    
    # Clean up
    await gazetteer_service.disconnect()

if __name__ == "__main__":
    asyncio.run(main()) 