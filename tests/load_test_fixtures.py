import csv
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def process_geometry(geom_str):
    """Convert ENVELOPE format to GeoJSON polygon."""
    if not geom_str or geom_str == "null":
        return None
    
    if geom_str.startswith("ENVELOPE"):
        # Extract coordinates from ENVELOPE(minX, maxX, maxY, minY)
        coords = geom_str.replace("ENVELOPE(", "").replace(")", "").split(",")
        minX, maxX, maxY, minY = map(float, coords)
        
        # Create a polygon from the envelope coordinates
        return {
            "type": "polygon",
            "coordinates": [[
                [minX, minY],
                [maxX, minY],
                [maxX, maxY],
                [minX, maxY],
                [minX, minY]
            ]]
        }
    return None

def load_fixtures():
    es = Elasticsearch("http://localhost:9200")
    
    # Delete existing test index if it exists
    if es.indices.exists(index="data_api_test"):
        es.indices.delete(index="data_api_test")
    
    # Create index with proper mapping
    mapping = {
        "mappings": {
            "properties": {
                "locn_geometry": {
                    "type": "geo_shape",
                    "orientation": "counterclockwise",
                    "coerce": True
                },
                "dcat_bbox": {
                    "type": "geo_shape",
                    "orientation": "counterclockwise",
                    "coerce": True
                },
                "dcat_centroid": {
                    "type": "geo_point"
                }
            }
        }
    }
    es.indices.create(index="data_api_test", body=mapping)
    
    # Read and process CSV file
    actions = []
    with open("tests/fixtures/gbl_fixtures_data.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Process geometry fields
            row["locn_geometry"] = process_geometry(row["locn_geometry"])
            row["dcat_bbox"] = process_geometry(row["dcat_bbox"])
            
            # Convert string references to JSON if present
            if row["dct_references_s"]:
                try:
                    row["dct_references_s"] = json.loads(row["dct_references_s"])
                except json.JSONDecodeError:
                    pass
            
            # Prepare document for bulk indexing
            action = {
                "_index": "data_api_test",
                "_id": row["id"],
                "_source": row
            }
            actions.append(action)
    
    # Bulk index the documents
    success, failed = bulk(es, actions)
    print(f"Successfully indexed {success} documents")
    if failed:
        print(f"Failed to index {len(failed)} documents")

if __name__ == "__main__":
    load_fixtures() 