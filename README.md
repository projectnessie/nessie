# Nessie

[![Build Status](https://github.com/projectnessie/nessie/workflows/Java%20CI%20with%20Maven/badge.svg)](https://github.com/projectnessie/nessie/actions)
[![codecov](https://codecov.io/gh/projectnessie/nessie/branch/master/graph/badge.svg?token=W9J9ZUYO1Y)](https://codecov.io/gh/projectnessie/nessie)



Nessie is a metadata store for [Iceberg](https://iceberg.incubator.apache.org/) that can function the same as the
Hive Metastore for Iceberg tables. It provides atomic transactions for all Iceberg operations on all Data Lake sources.
It is a viable alternative to Hive for use cases where no Hive metastore exists or Hive is too heavyweight of a dependency.
Nessie is a lightweight and low resource service that can run standalone, embedded in another application or in a
 serverless environment with a flexible backing database (eg DynamoDB or other cloud stores).

Iceberg alley service is highly scalable and can be scaled horizontally, limited only by the backend database.

Nessie also adds some features not found in other metadata stores including:
* tags and references - these allow for time based or human readable cross-table snapshots and time travel
* notifications - applications can be alerted immediately to metadata changes
* a simple React based UI 
* pluggable authentication and database backends
* support as an external metadata store for Delta Lake tables

Nessie is composed of several components:
* **Server** - the Nessie metadata store. A standard REST api. This can be deployed as part
of another java based application, as a standalone service or as a serverless application
* **Client** - the Nessie client for use in Iceberg, Delta lake
* **UI** - the React application. This can be run as a standalone SPA or as part of another application. Currently
it can be served as a static site
* **Distribution** - A full web application. This provides a full experience with the UI, REST api and openapi endpoints
in a single package
* **jgit** - the backend model that allows branching and commits in Nessie
* **Backends** - modules for different database backends depending on how the service is running
* **Delta Lake** - experimental Delta Lake `LogStore` using Nessie for atomic metadata updates
* **Serverless** - AWS Lambda for Nessie w/ IAM support.

## Requirements

- JDK 11 or higher: JDK11 or higher is needed to build Nessie although artifacts are still compatible with Java 8

## Installation

Clone this repository and run maven:
```bash
git clone https://github.com/dremio/nessie
cd nessie
./mvnw clean install
```

## Distribution
To run:
1. configuration in `servers/quarkus-server/src/main/resources/application.properties`
2. execute `./mvnw quarkus:dev`
3. go to `http://localhost:19120`

## UI 
To run the ui (from `ui` directory):
1. If you are running in test ensure that `setupProxy.js` points to the correct api instance. This ensures we avoid CORS
issues in testing
2. `npm install` will install dependencies
3. `npm run start` to start the ui in development mode via node

To deploy the ui (from `ui` directory):
1. `npm install` will install dependencies
2. `npm build` will minify and collect the package for deployment in `build`
3. the `build` directory can be deployed to any static hosting environment or run locally as `serve -s build`

## Docker image

When running `mvn clean install` a docker image will be created at `projectnessie/nessie` which can be started 
with `docker run -p 19120:19120 projectnessie/nessie` and the relevant environment variables. Environment variables
are specified as per https://github.com/eclipse/microprofile-config/blob/master/spec/src/main/asciidoc/configsources.asciidoc#default-configsources  

## Server
The server can be run from any jax-rs app by adding the following maven dependency:

```xml
<dependency>
  <groupId>com.dremio.nessie</groupId>
  <artifactId>nessie-services</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```
The classes in `servers/services/src/main/java/com/dremio/nessie/services/rest` must be registered to 
run in your app with VersionStore etc being injected at runtime.

You can also deploy to AWS lambda function by following the steps in `servers/lambda/README.md`

## Clients

The client has to be available to Iceberg or Delta Lake in order to be used. 
 * todo spark example
 * todo python example
 * todo java example
 
