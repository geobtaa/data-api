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

## TODO

X = done; O = in progress

- [X] Docker Image - Published on Docker Hub
- [X] Search - basic search across all text fields
- [X] Search - more complex search with filters
- [X] Search - pagination
- [X] Search - faceting
- [X] Search - sorting
- [X] Search - autocomplete
- [X] Search - thumbnail images (needs improvements)
- [ ] Search - spelling suggestions
- [ ] Search - advanced/fielded search
- [ ] Search - facet alpha and numerical pagination, and search within facets
- [ ] Search - facet include/exclude
- [X] Item View - citations
- [X] Item View - downloads
- [O] Item View - relations (triplestore)
- [ ] Item View - exports (Shapefile, CSV, GeoJSON)
- [ ] Item View - code previews (Py, R, Leaflet)
- [ ] Item View - embeds
- [ ] Item View - allmaps integration
- [ ] Item View - data dictionaries
- [ ] Item View - web services
- [ ] Item View - metadata
- [ ] Item View - related items (vector metadata search)
- [ ] Item View - similar images (vector imagery search)
- [ ] Gazetteer - Geonames
- [ ] Gazetteer - Who's on First
- [ ] Gazetteer - USGS Geographic Names Information System (GNIS)
- [ ] GeoJSONs
