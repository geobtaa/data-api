from typing import Dict, Union
import json
from ..viewers import ItemViewer


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

        # Add geometry if present
        if hasattr(document, "locn_geometry"):
            geom = document.locn_geometry
        else:
            geom = document.get("locn_geometry")

        if geom:
            refs["locn_geometry"] = geom

        return refs
    except Exception:
        return {}


def create_viewer_attributes(document: Union[Dict, object]) -> Dict:
    """Create viewer attributes from the document."""
    # Convert Record to dict if needed
    if not isinstance(document, dict):
        document = dict(document)

    references = parse_references(document)
    viewer = ItemViewer(references)

    return {
        "ui_viewer_protocol": viewer.viewer_protocol(),
        "ui_viewer_endpoint": viewer.viewer_endpoint(),
        "ui_viewer_geometry": viewer.viewer_geometry(),
    }
