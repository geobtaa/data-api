import json
import logging
from typing import Dict, Union

from ..viewers import ItemViewer

logger = logging.getLogger(__name__)


def parse_references(document: Union[Dict, object]) -> Dict:
    """Parse references from the document."""
    try:
        # Handle both Record and dict objects
        if hasattr(document, "__getitem__"):
            refs = document.get("dct_references_s", {})
        else:
            refs = getattr(document, "dct_references_s", {})

        # If refs is a string, try to parse it as JSON
        if isinstance(refs, str):
            try:
                refs = json.loads(refs)
            except json.JSONDecodeError:
                refs = {}
        elif not isinstance(refs, dict):
            refs = {}

        # Add geometry if present
        if hasattr(document, "locn_geometry"):
            geom = document.locn_geometry
        else:
            geom = document.get("locn_geometry")

        if geom:
            refs["locn_geometry"] = geom

        return refs
    except Exception as e:
        logger.error(f"Error parsing references: {str(e)}", exc_info=True)
        return {}


def create_viewer_attributes(document: Union[Dict, object]) -> Dict:
    """Create viewer attributes from the document."""
    # Convert Record to dict if needed
    if not isinstance(document, dict):
        document = dict(document)

    references = parse_references(document)
    logger.debug(f"Parsed references: {json.dumps(references, indent=2)}")

    viewer = ItemViewer(references)

    try:
        geometry = viewer.viewer_geometry()
        logger.debug(f"Viewer geometry: {json.dumps(geometry, indent=2)}")
    except Exception as e:
        logger.error(f"Error getting viewer geometry: {str(e)}", exc_info=True)
        geometry = None

    return {
        "ui_viewer_protocol": viewer.viewer_protocol(),
        "ui_viewer_endpoint": viewer.viewer_endpoint(),
        "ui_viewer_geometry": geometry,
    }


class ViewerService:
    """Service for handling viewer-related functionality."""

    def __init__(self, document: Union[Dict, object]):
        """Initialize the service with a document."""
        self.document = document
        self.references = parse_references(document)
        self.viewer = ItemViewer(self.references)

    def get_viewer_attributes(self) -> Dict:
        """Get all viewer attributes for the document."""
        try:
            geometry = self.viewer.viewer_geometry()
            logger.debug(f"Viewer geometry: {json.dumps(geometry, indent=2)}")
        except Exception as e:
            logger.error(f"Error getting viewer geometry: {str(e)}", exc_info=True)
            geometry = None

        return {
            "ui_viewer_protocol": self.viewer.viewer_protocol(),
            "ui_viewer_endpoint": self.viewer.viewer_endpoint(),
            "ui_viewer_geometry": geometry,
        }
