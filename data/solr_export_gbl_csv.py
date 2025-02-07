# Blacklight Solr

import json

host = "http://localhost:8983/solr/blacklight-core/select?"
params = [
    "indent=true",
    "q=*%3A*",
    "wt=csv",
    "rows=100",
]

# Load fields from JSON Schema
with open('aardvark_json_schema.json', 'r') as schema_file:
    schema = json.load(schema_file)
    fields = schema.get('properties', [])

separator = "|"

# Build the query string
query_string = f"{host}&{'&'.join(params)}&fl={','.join(fields)}&csv.mv.separator={separator}"

print(query_string)