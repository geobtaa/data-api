# Elasticsearch

## Indexing

### Indexing all documents
`curl -X POST http://localhost:8000/api/v1/index`

### Indexing a single document
`curl -X POST http://localhost:8000/api/v1/index?id=123`

## Searching

### Searching all documents
`curl -X GET http://localhost:9200/geoblacklight/_search?q=*:*&pretty`

curl -X GET http://elasticsearch:9200/geoblacklight/_search?q=*:*&pretty

### View mapping
`curl -X GET http://localhost:9200/geoblacklight/_mapping?pretty`

## Troubleshooting

### Running out of space? Elasticsearch will be upset.

ES will turn read-only if it runs out of disk space. This is a problem because it means that the index is no longer writable.

To fix this, we can disable the disk space threshold and allow the index to be deleted.

```bash
curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_cluster/settings -d '{ "transient": { "cluster.routing.allocation.disk.threshold_enabled": false } }'

curl -XPUT -H "Content-Type: application/json" http://localhost:9200/_all/_settings -d '{"index.blocks.read_only_allow_delete": null}'
```