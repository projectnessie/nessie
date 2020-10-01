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
and types of operations should be allowed.

It is planned to introduce a comprehensive RBAC policies.
