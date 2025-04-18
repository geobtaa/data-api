import pytest
from app.viewers import ItemViewer


def test_viewer_protocol_with_cog():
    references = {"https://github.com/cogeotiff/cog-spec": "https://example.com/cog.tif"}
    viewer = ItemViewer(references)
    assert viewer.viewer_protocol() == "cog"
    assert viewer.viewer_endpoint() == "https://example.com/cog.tif"


def test_viewer_protocol_with_wms():
    references = {"http://www.opengis.net/def/serviceType/ogc/wms": "https://example.com/wms"}
    viewer = ItemViewer(references)
    assert viewer.viewer_protocol() == "wms"
    assert viewer.viewer_endpoint() == "https://example.com/wms"


def test_viewer_protocol_with_no_references():
    viewer = ItemViewer({})
    assert viewer.viewer_protocol() == "geo_json"
    assert viewer.viewer_endpoint() == ""


def test_viewer_geometry_with_envelope():
    references = {"locn_geometry": "ENVELOPE(-180, 180, 90, -90)"}
    viewer = ItemViewer(references)
    geometry = viewer.viewer_geometry()
    assert geometry["type"] == "Polygon"
    assert geometry["coordinates"] == [
        [
            [-180, 90],  # top left
            [-180, -90],  # bottom left
            [180, -90],  # bottom right
            [180, 90],  # top right
            [-180, 90],  # close the polygon
        ]
    ]


def test_viewer_geometry_with_polygon():
    references = {"locn_geometry": "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))"}
    viewer = ItemViewer(references)
    geometry = viewer.viewer_geometry()
    assert geometry["type"] == "Polygon"
    assert geometry["coordinates"] == [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]


def test_viewer_geometry_with_geojson():
    references = {"locn_geometry": '{"type": "Point", "coordinates": [0, 0]}'}
    viewer = ItemViewer(references)
    geometry = viewer.viewer_geometry()
    assert geometry["type"] == "Point"
    assert geometry["coordinates"] == [0, 0]


def test_viewer_geometry_with_invalid():
    references = {"locn_geometry": "INVALID"}
    viewer = ItemViewer(references)
    assert viewer.viewer_geometry() is None
