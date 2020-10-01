# Security


## Authentication

Nessie currently supports 3 security modes:

* No Security
* Open Id Connect
* AWS IAM Roles (limited to API calls, UI not supported)


## Authorization

Nessie authorization can only be done externally at the moment. However, because of 
the way that the REST APIs are defined, many operations can be controlled via a layer 
7 firewall so that users and systems can be controlled depending on what read/write 
and types of operations should be allowed. This works especially well with Nessie run 
as a AWS Lambda using [API gateway policies](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-iam-policy-examples.html).
