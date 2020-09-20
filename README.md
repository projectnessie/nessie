# Nessie

[![Build Status](https://github.com/projectnessie/nessie/workflows/Java%20CI%20with%20Maven/badge.svg)](https://github.com/projectnessie/nessie/actions)
[![codecov](https://codecov.io/gh/projectnessie/nessie/branch/master/graph/badge.svg?token=W9J9ZUYO1Y)](https://codecov.io/gh/projectnessie/nessie)

Nessie is a system to provide Git like capability for Iceberg Tables, Delta Lake Tables, Hive Tables and Sql Views.

More information can be found at [projectnessie.org](http://projectnessie.org/).

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
 
