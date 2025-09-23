/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
  `java-platform`
  `maven-publish`
  signing
  id("nessie-common-base")
}

publishingHelper { mavenName = "Nessie - Bill of Materials (BOM)" }

dependencies {
  constraints {
    api(rootProject)
    api(project(":nessie-azurite-testcontainer"))
    api(project(":nessie-cli"))
    api(project(":nessie-cli-grammar"))
    api(project(":nessie-client"))
    api(project(":nessie-client-testextension"))
    api(project(":nessie-combined-cs"))
    api(project(":nessie-compatibility-common"))
    api(project(":nessie-compatibility-tests"))
    api(project(":nessie-compatibility-jersey"))
    api(project(":nessie-container-spec-helper"))
    api(project(":nessie-doc-generator-annotations"))
    api(project(":nessie-doc-generator-doclet"))
    api(project(":nessie-events-api"))
    api(project(":nessie-events-spi"))
    api(project(":nessie-events-service"))
    api(project(":nessie-events-quarkus"))
    api(project(":nessie-events-ri"))
    api(project(":nessie-gc-base"))
    api(project(":nessie-gc-repository-jdbc"))
    api(project(":nessie-gcs-testcontainer"))
    api(project(":nessie-model"))
    api(project(":nessie-notice"))
    api(project(":nessie-jaxrs-testextension"))
    api(project(":nessie-jaxrs-tests"))
    api(project(":nessie-keycloak-testcontainer"))
    api(project(":nessie-minio-testcontainer"))
    api(project(":nessie-nessie-testcontainer"))
    api(project(":nessie-network-tools"))
    api(project(":nessie-object-storage-mock"))
    api(project(":nessie-quarkus-authn"))
    api(project(":nessie-quarkus-authz"))
    api(project(":nessie-quarkus-catalog"))
    api(project(":nessie-quarkus-distcache"))
    api(project(":nessie-quarkus-common"))
    api(project(":nessie-quarkus-config"))
    api(project(":nessie-quarkus-ext-deployment"))
    api(project(":nessie-quarkus-ext"))
    api(project(":nessie-quarkus-rest"))
    api(project(":nessie-quarkus-secrets"))
    api(project(":nessie-server-admin-tool"))
    api(project(":nessie-quarkus"))
    api(project(":nessie-quarkus-tests"))
    api(project(":nessie-rest-common"))
    api(project(":nessie-rest-services"))
    api(project(":nessie-services"))
    api(project(":nessie-services-config"))
    api(project(":nessie-server-store"))
    api(project(":nessie-server-store-proto"))
    api(project(":nessie-storage-uri"))
    api(project(":nessie-content-generator"))
    api(project(":nessie-protobuf-relocated"))
    api(project(":nessie-tasks-api"))
    api(project(":nessie-tasks-service-async"))
    api(project(":nessie-tasks-service-impl"))
    api(project(":nessie-trino-testcontainer"))
    api(project(":nessie-versioned-spi"))
    api(project(":nessie-versioned-storage-batching"))
    api(project(":nessie-versioned-storage-bigtable"))
    api(project(":nessie-versioned-storage-bigtable-tests"))
    api(project(":nessie-versioned-storage-cache"))
    api(project(":nessie-versioned-storage-cassandra"))
    api(project(":nessie-versioned-storage-cassandra-tests"))
    api(project(":nessie-versioned-storage-cassandra2"))
    api(project(":nessie-versioned-storage-cassandra2-tests"))
    api(project(":nessie-versioned-storage-cleanup"))
    api(project(":nessie-versioned-storage-common"))
    api(project(":nessie-versioned-storage-common-proto"))
    api(project(":nessie-versioned-storage-common-serialize"))
    api(project(":nessie-versioned-storage-common-tests"))
    api(project(":nessie-versioned-storage-dynamodb"))
    api(project(":nessie-versioned-storage-dynamodb-tests"))
    api(project(":nessie-versioned-storage-dynamodb2"))
    api(project(":nessie-versioned-storage-dynamodb2-tests"))
    api(project(":nessie-versioned-storage-inmemory"))
    api(project(":nessie-versioned-storage-inmemory-tests"))
    api(project(":nessie-versioned-storage-jdbc"))
    api(project(":nessie-versioned-storage-jdbc-tests"))
    api(project(":nessie-versioned-storage-jdbc2"))
    api(project(":nessie-versioned-storage-jdbc2-tests"))
    api(project(":nessie-versioned-storage-mongodb"))
    api(project(":nessie-versioned-storage-mongodb-tests"))
    api(project(":nessie-versioned-storage-mongodb2"))
    api(project(":nessie-versioned-storage-mongodb2-tests"))
    api(project(":nessie-versioned-storage-rocksdb"))
    api(project(":nessie-versioned-storage-rocksdb-tests"))
    api(project(":nessie-versioned-storage-store"))
    api(project(":nessie-versioned-storage-testextension"))
    api(project(":nessie-versioned-tests"))
    api(project(":nessie-versioned-transfer-proto"))
    api(project(":nessie-versioned-transfer-related"))
    api(project(":nessie-versioned-transfer"))

    // Nessie Data Catalog
    api(project(":nessie-catalog-files-api"))
    api(project(":nessie-catalog-files-impl"))
    api(project(":nessie-catalog-format-iceberg"))
    api(project(":nessie-catalog-format-iceberg-fixturegen"))
    api(project(":nessie-catalog-model"))
    api(project(":nessie-catalog-service-common"))
    api(project(":nessie-catalog-service-config"))
    api(project(":nessie-catalog-service-rest"))
    api(project(":nessie-catalog-service-impl"))
    api(project(":nessie-catalog-service-transfer"))
    api(project(":nessie-catalog-secrets-api"))
    api(project(":nessie-catalog-secrets-smallrye"))
    api(project(":nessie-catalog-secrets-cache"))
    api(project(":nessie-catalog-secrets-aws"))
    api(project(":nessie-catalog-secrets-gcs"))
    api(project(":nessie-catalog-secrets-azure"))
    api(project(":nessie-catalog-secrets-vault"))

    if (!isIncludedInNesQuEIT()) {
      api(project(":nessie-gc-iceberg"))
      api(project(":nessie-gc-iceberg-files"))
      api(project(":nessie-gc-iceberg-mock"))
      api(project(":nessie-gc-tool"))

      val ideSyncActive =
        System.getProperty("idea.sync.active").toBoolean() ||
          System.getProperty("idea.active").toBoolean() ||
          System.getProperty("eclipse.product") != null ||
          gradle.startParameter.taskNames.any { it.startsWith("eclipse") }
      val sparkVersions = rootProject.extra["sparkVersions"].toString().split(",").map { it.trim() }
      val allScalaVersions = LinkedHashSet<String>()
      for (sparkVersion in sparkVersions) {
        val scalaVersions =
          rootProject.extra["sparkVersion-${sparkVersion}-scalaVersions"]
            .toString()
            .split(",")
            .map { it.trim() }
        for (scalaVersion in scalaVersions) {
          allScalaVersions.add(scalaVersion)
          api(project(":nessie-spark-extensions-${sparkVersion}_$scalaVersion"))
          if (ideSyncActive) {
            break
          }
        }
      }
      for (scalaVersion in allScalaVersions) {
        api(project(":nessie-spark-extensions-base_$scalaVersion"))
        if (ideSyncActive) {
          break
        }
      }
    }
  }
}
