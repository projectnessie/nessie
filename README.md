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
1. configure port, backend, security etc in `distribution/src/main/resources/config.yaml`
2. execute `./mvnw exec:exec -pl :nessie-distribution`
3. go to `http://localhost:19120`
4. Default user (when using basic backend is admin_user:test123)

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

## Server
The server can be run from any jax-rs app by adding the following maven dependency:

```xml
<dependency>
  <groupId>com.dremio.nessie</groupId>
  <artifactId>nessie-server</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```
And adding the following to your Server:

```java
RestServerV1 restServer = new RestServerV1();
final ServletHolder restHolder = new ServletHolder(new ServletContainer(restServer));
restHolder.setInitOrder(2);
servletContextHandler.addServlet(restHolder, "/api/v1/*");
```

You can also deploy to AWS lambda function by deploying the `nessie-serverless-1.0-SNAPSHOT.jar` as a lambda 
function

## Clients

The client has to be available to Iceberg or Delta Lake in order to be used. 
 * todo spark example
 * todo python example
 * todo java example
 
## Database backends

Any database can be used as the backend. Currently, DynamoDb is implemented and there is a simple in memory
database for testing. To implement a new backend simply implement `com.dremio.nessie.backend.Backend` and ensure the 
resulting class can be found on the classpath. Considerations for implementing a backend:

* must be able to support transactions or optimistic updates. 
* shouldn't be eventually consistent
* should be scalable

The Nessie service expects that if a create or update operation completes successfully then it should be atomic 
and immediately available to all users.

## Authentication service

Authentication and authorization is also pluggable. Currently there is a test only auth service for testing purposes only
there is also a database backed service using json web tokens (JWT). The password is stored in the db as a bcrypt hashed 
and salted string. A JWT token is generated and is valid for 15 minutes by default. 

To create a new backend implement `com.dremio.nessie.auth.UserService` and add it to the classpath.

