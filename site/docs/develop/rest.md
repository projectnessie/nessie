# Rest API

Nessie's REST APIs are how all applications interact with Nessie. The APIs are specified 
according to the openapi v3 standard and are available when running the server by at the path
`/nessie-openapi/openapi.yaml` (for example via `curl http://127.0.0.1:19120/nessie-openapi/openapi.yaml`)
and on the Download/Release pages.

List endpoints accept a CEL `filter` query parameter. See [CEL filters](cel-filters.md)
for the bound variables and examples.
