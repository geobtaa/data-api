# data-api
FastAPI JSON:API for BTAA Aardvark Metadata

![Data API](docs/data-api.png)

## Development

Install dependencies:
```bash
pip install -r requirements.txt
```

The default settings should work for local development, but if you need to tweak the environment variables, you can do so by copying the `.env.example` file to `.env` and making your changes.

```bash
cp .env.example .env
```

Run the Docker containers:

* [ParadeDB](https://www.paradedb.com/)
* [Elasticsearch](https://www.elastic.co/elasticsearch/)
* [Redis](https://redis.io/) (for caching)

```bash
docker compose up -d
```

Import the GeoBlacklight test fixture data:
```bash
cd data
gunzip geoblacklight_development.gz
psql -h localhost -p 2345 -U postgres -d geoblacklight_development -f geoblacklight_development
```

Run the API server:
```bash
uvicorn main:app --reload
```

## Docker Hub

The application is also available as a Docker image on Docker Hub. You can pull and run the image using the following commands:

```bash
docker pull ewlarson/ogm-api:latest
docker run -d -p 8000:8000 ewlarson/ogm-api:latest
```

This will start the API server on port 8000.

## Endpoints

### GET /docs

[http://localhost:8000/docs](http://localhost:8000/docs)

Returns the API documentation.

## Caching

The API supports aggressive Redis-based caching to improve performance. Caching can be controlled through environment variables:

```
# Enable/disable caching
ENDPOINT_CACHE=true

# Redis connection settings
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=optional_password
REDIS_DB=0

# Cache TTL settings (in seconds)
DOCUMENT_CACHE_TTL=86400  # 24 hours
SEARCH_CACHE_TTL=3600     # 1 hour
SUGGEST_CACHE_TTL=7200    # 2 hours 
LIST_CACHE_TTL=43200      # 12 hours
CACHE_TTL=43200           # Default TTL (12 hours)
```

When caching is enabled:
- API responses are cached in Redis based on the endpoint and its parameters
- Search results are cached for faster repeated queries
- Document details are cached to reduce database load
- Suggestions are cached to improve autocomplete performance

The cache is automatically invalidated when:
- Documents are created, updated, or deleted
- The Elasticsearch index is rebuilt

You can manually clear the cache using:
```
GET /api/v1/cache/clear?cache_type=search|document|suggest|all
```

## Colophon

### Gazetteers

#### BTAA Spatial Metadata

Data from BTAA GIN.

#### GeoNames

Data from GeoNames. [License - CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

#### Who's On First

Data from Who's On First. [License](https://whosonfirst.org/docs/licenses/)

## TODO

X = done; O = in progress

- [X] Docker Image - Published on Docker Hub
- [X] Search - basic search across all text fields
- [X] Search - autocomplete
- [X] Search - spelling suggestions
- [X] Search - more complex search with filters
- [X] Search - pagination
- [X] Search - sorting
- [X] Search - basic faceting
- [X] Performance - Redis caching
- [ ] Search - facet include/exclude
- [ ] Search - facet alpha and numerical pagination, and search within facets
- [ ] Search - advanced/fielded search
- [ ] Search - spatial search
- [X] Search Results - thumbnail images (needs improvements)
- [X] Search Results - bookmarked items
- [X] Item View - citations
- [X] Item View - downloads
- [X] Item View - relations (triplestore)
- [ ] Item View - exports (Shapefile, CSV, GeoJSON)
- [ ] Item View - export conversions (Shapefile to: GeoJSON, CSV, TSV, etc)
- [ ] Item View - code previews (Py, R, Leaflet)
- [ ] Item View - embeds
- [ ] Item View - allmaps integration
- [ ] Item View - data dictionaries
- [ ] Item View - web services
- [ ] Item View - metadata
- [ ] Item View - related items (vector metadata search)
- [ ] Item View - similar images (vector imagery search)
- [X] Gazetteer - BTAA Spatial
- [X] Gazetteer - Geonames
- [X] Gazetteer - Who's on First
- [ ] Gazetteer - USGS Geographic Names Information System (GNIS)
- [ ] GeoJSONs
