from typing import Dict
import json
from ..viewers import ItemViewer


def parse_references(document: Dict) -> Dict:
    """Parse references from the document."""
    try:
        references = (
            json.loads(document["dct_references_s"]) if document["dct_references_s"] else {}
        )
        if document["locn_geometry"]:
            references["locn_geometry"] = document["locn_geometry"]
        return references
    except json.JSONDecodeError:
        return {}


def create_viewer_attributes(document: Dict) -> Dict:
    """Create viewer attributes from the document."""
    references = parse_references(document)
    viewer = ItemViewer(references)
    return {
        "ui_viewer_protocol": viewer.viewer_protocol(),
        "ui_viewer_endpoint": viewer.viewer_endpoint(),
        "ui_viewer_geometry": viewer.viewer_geometry(),
    }
