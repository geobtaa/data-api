from typing import Dict, Optional
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
                return references.get("http://schema.org/url") or references.get("http://schema.org/downloadUrl")
            except json.JSONDecodeError:
                return None
        return None

    def get_citation(self) -> str:
        """Generate a simple citation string."""
        try:
            parts = []

            # Creators
            creators = self.document.get("dct_creator_sm", [])
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
            resource_type = self.document.get("gbl_resourcetype_sm", [""])[0]
            if resource_type.lower() in ['datasets', 'web services']:
                if provider := self.document.get("schema_provider_s"):
                    parts.append(f"{provider}.")
            else:
                if publishers := self.document.get("dct_publisher_sm"):
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
            return "Citation unavailable" 