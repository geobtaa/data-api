from typing import Dict, Optional, TypedDict, Union, List
import json
import re

class Reference(TypedDict):
    protocol: str
    endpoint: str

class GeoJSON(TypedDict):
    type: str
    coordinates: Union[List[List[List[float]]], List[float]]

class ItemViewer:
    REFERENCE_URI_TO_NAME = {
        'urn:x-esri:serviceType:ArcGIS#DynamicMapLayer': 'arcgis_dynamic_map_layer',
        'urn:x-esri:serviceType:ArcGIS#FeatureLayer': 'arcgis_feature_layer',
        'urn:x-esri:serviceType:ArcGIS#ImageMapLayer': 'arcgis_image_map_layer',
        'urn:x-esri:serviceType:ArcGIS#TiledMapLayer': 'arcgis_tiled_map_layer',
        'https://github.com/cogeotiff/cog-spec': 'cog',
        'http://lccn.loc.gov/sh85035852': 'documentation_download',
        'http://schema.org/url': 'documentation_external',
        'http://schema.org/downloadUrl': 'download',
        'http://geojson.org/geojson-spec.html': 'geo_json',
        'http://iiif.io/api/image': 'iiif_image',
        'http://iiif.io/api/presentation#manifest': 'iiif_manifest',
        'http://schema.org/image': 'image',
        'http://www.opengis.net/cat/csw/csdgm': 'metadata_fgdc',
        'http://www.w3.org/1999/xhtml': 'metadata_html',
        'http://www.isotc211.org/schemas/2005/gmd/': 'metadata_iso',
        'http://www.loc.gov/mods/v3': 'metadata_mods',
        'https://oembed.com': 'oembed',
        'https://openindexmaps.org': 'open_index_map',
        'https://github.com/protomaps/PMTiles': 'pmtiles',
        'http://schema.org/thumbnailUrl': 'thumbnail',
        'https://wiki.osgeo.org/wiki/Tile_Map_Service_Specification': 'tile_map_service',
        'https://github.com/mapbox/tilejson-spec': 'tile_json',
        'http://www.opengis.net/def/serviceType/ogc/wcs': 'wcs',
        'http://www.opengis.net/def/serviceType/ogc/wfs': 'wfs',
        'http://www.opengis.net/def/serviceType/ogc/wmts': 'wmts',
        'http://www.opengis.net/def/serviceType/ogc/wms': 'wms',
        'https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames': 'xyz_tiles'
    }

    def __init__(self, references: Dict[str, str]):
        self.references = references

    def viewer_protocol(self) -> Optional[str]:
        preference = self._viewer_preference()
        return self.REFERENCE_URI_TO_NAME.get(preference['protocol']) if preference else 'geo_json'

    def viewer_endpoint(self) -> str:
        preference = self._viewer_preference()
        return preference['endpoint'] if preference else ""

    def _viewer_preference(self) -> Optional[Reference]:
        preferences = [
            self._get_reference('https://github.com/cogeotiff/cog-spec'),
            self._get_reference('https://github.com/protomaps/PMTiles'),
            self._get_reference('https://oembed.com'),
            self._get_reference('https://openindexmaps.org'),
            self._get_reference('https://github.com/mapbox/tilejson-spec'),
            self._get_reference('https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames'),
            self._get_reference('http://www.opengis.net/def/serviceType/ogc/wmts'),
            self._get_reference('https://wiki.osgeo.org/wiki/Tile_Map_Service_Specification'),
            self._get_reference('http://www.opengis.net/def/serviceType/ogc/wms'),
            self._get_reference('http://iiif.io/api/presentation#manifest'),
            self._get_reference('http://iiif.io/api/image'),
            self._get_reference('urn:x-esri:serviceType:ArcGIS#TiledMapLayer'),
            self._get_reference('urn:x-esri:serviceType:ArcGIS#DynamicMapLayer'),
            self._get_reference('urn:x-esri:serviceType:ArcGIS#ImageMapLayer'),
            self._get_reference('urn:x-esri:serviceType:ArcGIS#FeatureLayer')
        ]
        return next((pref for pref in preferences if pref is not None), None)

    def _get_reference(self, protocol: str) -> Optional[Reference]:
        endpoint = self.references.get(protocol)
        return {'protocol': protocol, 'endpoint': endpoint} if endpoint else None

    def viewer_geometry(self) -> Optional[GeoJSON]:
        """Convert locn_geometry to a GeoJSON object."""
        if not self.references.get('locn_geometry'):
            return None

        geometry = self.references['locn_geometry']
        
        # print(geometry)
        # Check if it's an ENVELOPE format

        
        envelope_match = re.match(r'ENVELOPE\(([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,\s*([-\d.]+)\)', geometry)

        # print(envelope_match)

        if envelope_match:
            # Extract coordinates from ENVELOPE(minx,maxx,maxy,miny)
            minx, maxx, maxy, miny = map(float, envelope_match.groups())
            # Create a polygon from the envelope coordinates
            return {
                "type": "Polygon",
                "coordinates": [[
                    [minx, maxy],  # top left
                    [minx, miny],  # bottom left
                    [maxx, miny],  # bottom right
                    [maxx, maxy],  # top right
                    [minx, maxy]   # close the polygon
                ]]
            }
        
        # Check if it's a POLYGON format
        polygon_match = re.match(r'POLYGON\(\(\s*([-\d.\s,]+)\s*\)\)', geometry)

        if polygon_match:
            # Extract coordinates from POLYGON((x1 y1, x2 y2, ..., xn yn))
            coordinates_str = polygon_match.group(1)
            # Split the coordinates and convert them to float pairs
            coordinates = [
                list(map(float, coord.split()))
                for coord in coordinates_str.split(',')
            ]
            # Ensure the polygon is closed by repeating the first point at the end
            if coordinates[0] != coordinates[-1]:
                coordinates.append(coordinates[0])
            return {
                "type": "Polygon",
                "coordinates": [coordinates]
            }
        
        # Try parsing as JSON (handling escaped quotes)
        try:
            # Replace escaped quotes and parse
            clean_geometry = geometry.replace('&quot;', '"')
            return json.loads(clean_geometry)
        except json.JSONDecodeError:
            return None