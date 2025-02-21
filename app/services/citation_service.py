from typing import Dict, Optional, List
import json
import logging

logger = logging.getLogger(__name__)


class CitationService:
    """Service for generating simple citations."""

    def __init__(self, document: Dict):
        self.document = document

    def _get_url(self) -> Optional[str]:
        """Get the primary URL for the document."""
        references = self.document.get("dct_references_s")
        if isinstance(references, str):
            try:
                references = json.loads(references)
                return references.get("http://schema.org/url") or references.get(
                    "http://schema.org/downloadUrl"
                )
            except json.JSONDecodeError:
                return None
        return None

    def _get_resource_type(self) -> str:
        """Get the resource type with error handling."""
        resource_types = self.document.get("gbl_resourcetype_sm", [])
        if isinstance(resource_types, list) and resource_types:
            return resource_types[0]
        return ""

    def _get_creators(self) -> List[str]:
        """Get creators with error handling."""
        creators = self.document.get("dct_creator_sm", [])
        if isinstance(creators, list):
            return creators
        return []

    def _get_publishers(self) -> List[str]:
        """Get publishers with error handling."""
        publishers = self.document.get("dct_publisher_sm", [])
        if isinstance(publishers, list):
            return publishers
        return []

    def get_citation(self) -> str:
        """Generate a simple citation string."""
        try:
            parts = []

            # Creators
            creators = self._get_creators()
            if creators:
                parts.append(f"{', '.join(creators)}.")
            else:
                parts.append("[Creator not found],")

            # Date
            date = self.document.get("dct_issued_s")
            parts.append(f"({date if date else 'n.d.'}).")

            # Title
            if title := self.document.get("dct_title_s"):
                parts.append(f"{title}.")

            # Publisher/Provider based on resource type
            resource_type = self._get_resource_type()
            if resource_type.lower() in ["datasets", "web services"]:
                if provider := self.document.get("schema_provider_s"):
                    parts.append(f"{provider}.")
            else:
                publishers = self._get_publishers()
                if publishers:
                    parts.append(f"{', '.join(publishers)}.")

            # URL
            if url := self._get_url():
                parts.append(url)

            # Resource type
            if resource_type:
                parts.append(f"({resource_type.lower().rstrip('s')})")

            citation = " ".join(parts)
            return citation

        except Exception as e:
            logger.error(f"Citation generation failed: {str(e)}")
            logger.error(f"Document: {self.document}")
            return "Citation unavailable"
