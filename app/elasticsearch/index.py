from .client import es
from db.database import database
from db.models import geoblacklight_development
import json
import re
import os

async def index_documents():
    """Index all documents from PostgreSQL into Elasticsearch."""
    index_name = os.getenv("ELASTICSEARCH_INDEX", "geoblacklight")
    
    if await es.indices.exists(index=index_name):
        await es.indices.delete(index=index_name)
    
    from .client import init_elasticsearch
    await init_elasticsearch()
    
    documents = await database.fetch_all(geoblacklight_development.select())
    bulk_data = prepare_bulk_data(documents, index_name)
    
    if bulk_data:
        return await perform_bulk_indexing(bulk_data, index_name)
    
    return {"message": "No documents to index"}

def prepare_bulk_data(documents, index_name):
    """Prepare documents for bulk indexing."""
    bulk_data = []
    for doc in documents:
        doc_dict = process_document(dict(doc))
        bulk_data.append({"index": {"_index": index_name, "_id": doc_dict["id"]}})
        bulk_data.append(doc_dict)
    return bulk_data

def process_document(doc_dict):
    """Process a single document for indexing."""
    for key, value in doc_dict.items():
        if isinstance(value, (list, tuple)):
            doc_dict[key] = list(value)
        elif key == "dct_references_s" and value:
            try:
                doc_dict[key] = json.loads(value)
            except json.JSONDecodeError:
                doc_dict[key] = value
        elif key == "locn_geometry":
            try:
                doc_dict[key] = process_geometry(value)
            except Exception as e:
                print(f"Error processing locn_geometry: {e}")
        elif key == "dcat_bbox":
            try:
                doc_dict[key] = process_geometry(value)
            except Exception as e:
                print(f"Error processing dcat_bbox: {e}")
    
    return doc_dict

def process_geometry(geometry):
    """Process geometry fields in a document."""
    try:
        envelope_match = re.match(r'ENVELOPE\(([-\d.]+),([-\d.]+),([-\d.]+),([-\d.]+)\)', geometry)
        if envelope_match:
            minx, maxx, maxy, miny = map(float, envelope_match.groups())
            geojson_geometry = {
                "type": "Polygon",
                "coordinates": [[
                    [minx, maxy], [minx, miny], [maxx, miny],
                    [maxx, maxy], [minx, maxy]
                ]]
            }
            return geojson_geometry
    except Exception as e:
        print(f"Error processing locn_geometry: {e}")

async def perform_bulk_indexing(bulk_data, index_name):
    """Perform bulk indexing operation."""
    response = await es.bulk(operations=bulk_data, refresh=True)
    
    if response.get('errors'):
        for item in response['items']:
            if 'error' in item['index']:
                print(f"Error indexing document {item['index']['_id']}: {item['index']['error']}")
    
    stats = await es.indices.stats(index=index_name)
    doc_count = stats["indices"][index_name]["total"]["docs"]["count"]
    return {"message": f"Successfully indexed {doc_count} documents"}
