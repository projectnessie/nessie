# Releases

## 0.2.1 Release (October 30, 2020)

* Fix missing dateutil requirement for pynessie install
* Address path discovery in Gradle plugin (for testing in external integrations)
 
## 0.2.0 Release (October 29, 2020)

* Update [Nessie CLI](../tools/cli.md) commands to better match `git` syntax
* Update [REST Apis](../develop/rest.md) to be more consistent and better
* Add support for merge & cherry-pick in DynamoDB storage backend
* Add [WebUI](../tools/ui.md)
* Introduce new DynamoDB optimizations to support faster log and entry retrieval
* Update to Quarkus 1.9.1 
* Expose the new [Store interface](https://github.com/projectnessie/nessie/blob/main/versioned/dynamodb/src/main/java/com/dremio/nessie/versioned/store/Store.java) for low level storage implementations
* Introduce Quarkus Gradle runner plugin for easier third-party testing (e.g. Iceberg)
* Enable [swagger-ui](../tools/ui.md) by default in Nessie service 

## 0.1.0 Release (October 1, 2020)

* Initial release