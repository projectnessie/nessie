# Nessie Iceberg REST Server

## Overview

Quarkus server implementation for the Nessie REST Catalog, which also implements the Iceberg REST protocol.

Rough system "architecture":
```
  Iceberg-client --> Nessie REST Catalog Server --> Nessie server(s)
        |                 |
        \/                \/
    File/Object       File/Object
       store             store
    
```

## Status

* prototype
* no authentication
* no authorization
* no proper warehouse configuration

## Try it

1. Checkout and build Iceberg from https://github.com/snazy/iceberg/tree/iceberg-nesqueit, which 
   contains required namespace related fixed in Iceberg test code. Or use the current 'master'
   branch, once the above has been merged.
   ```bash
   # For Apache Iceberg Git clone
   ./gradlew publishToMavenLocal
   ```
2. Build the Quarkus servers
   ```bash
   # From the Nessie Git clone
   ./gradlew :nessie-quarkus:quarkusBuild :nessie-rest-catalog-server:quarkusBuild
   ```
3. Start a Nessie Quarkus server (listens on http://localhost:19120/) from `:nessie-quarkus`
   ```bash
   # From the Nessie Git clone
   java -jar servers/quarkus-server/build/quarkus-app/quarkus-run.jar
   ```
4. Start a Nessie-REST-Catalog Quarkus server (listens on http://localhost:19130/)
   from `:nessie-iceberg-rest-server`
   ```bash
   # From the Nessie Git clone
   java -jar rest-catalog/quarkus-rest-catalog/build/quarkus-app/quarkus-run.jar
   ```
5. Configure the Iceberg client using these Iceberg catalog properties:
   ```
   catalog-impl = org.apache.iceberg.rest.RESTCatalog
   uri = http://127.0.0.1:19130/iceberg
   prefix = main
   io-impl = org.apache.iceberg.io.ResolvingFileIO
   credential = <client-id>:<client-secret>
   ```
   Note: `prefix` is used to configure the Nessie reference name (the default `main` branch in the
   snippet above). Also, the configured client-id must have the `catalog` scope.
