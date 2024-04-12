---
title: "Security"
---

# Security


## Authentication

Nessie currently supports 3 security modes:

* No Security
* Open Id Connect
* AWS IAM Roles (supported only on the Nessie client side)


## Authorization

Nessie authorization can only be done externally at the moment. However, because of 
the way that the REST APIs are defined, many operations can be controlled via a layer 
7 firewall so that users and systems can be controlled depending on what read/write 
and types of operations should be allowed.

## Metadata authorization

Nessie supports authorization on metadata. Details are described in the [Metadata Authorization](authorization.md) section.
