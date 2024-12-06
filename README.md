# data-api
FastAPI JSON:API for BTAA Aardvark Metadata

## Development

Install dependencies:
```bash
pip install -r requirements.txt
```

Run the server:
```bash
uvicorn main:app --reload
```

## Endpoints

### GET /docs

[http://localhost:8000/docs](http://localhost:8000/docs)

Returns the API documentation.

### GET /api/v1/documents

[http://localhost:8000/api/v1/documents](http://localhost:8000/api/v1/documents)

Returns a paginated list of documents.


