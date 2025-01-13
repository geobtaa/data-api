# Elasticsearch

## Indexing

### Indexing all documents
`curl -X POST http://localhost:8000/api/v1/index`

### Indexing a single document
`curl -X POST http://localhost:8000/api/v1/index?id=123`

## Searching

### Searching all documents
`curl -X GET http://localhost:9200/geoblacklight/_search?q=*:*&pretty`

### View mapping
`curl -X GET http://localhost:9200/geoblacklight/_mapping?pretty`
