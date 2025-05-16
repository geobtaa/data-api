# To Migrate Existing GBL Aardvark Data from Solr

Simply export it as CSV, and delimit multi-value fields with a pipe:

```
http://localhost:8983/solr/blacklight-core/select?csv.mv.separator=%7C&indent=true&q.op=OR&q=*%3A*&wt=csv&rows=100000
```

