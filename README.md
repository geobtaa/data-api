# data-api
FastAPI JSON:API for BTAA Aardvark Metadata

![Data API](docs/data-api.png)

## Development

Install dependencies:
```bash
pip install -r requirements.txt
```

Configure the database:
```bash
cp .env.example .env
```

Run the server:
```bash
uvicorn main:app --reload
```

## Endpoints

### GET /openapi.json

[http://localhost:8000/openapi.json](http://localhost:8000/openapi.json)

Returns the OpenAPI schema.

### GET /docs

[http://localhost:8000/docs](http://localhost:8000/docs)

Returns the API documentation.

### GET /api/v1/documents

[http://localhost:8000/api/v1/documents](http://localhost:8000/api/v1/documents)

Returns a paginated list of documents.

## Elasticsearch

```bash
docker compose up -d
```

## TODO

- [X] Search - basic search across all text fields
- [X] Search - more complex search with filters
- [X] Search - pagination
- [X] Search - faceting
- [X] Search - sorting
- [X] Search - autocomplete
- [ ] Search - spelling suggestions
- [ ] Search - fielded search
- [ ] Search - thumbnail images
- [ ] Item View - citations
- [ ] Item View - Allmaps