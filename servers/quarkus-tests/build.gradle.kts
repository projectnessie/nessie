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

plugins { id("nessie-conventions-java21") }

publishingHelper { mavenName = "Nessie - Quarkus Tests" }

dependencies {
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-storage-bigtable-tests"))
  implementation(project(":nessie-versioned-storage-cassandra-tests"))
  implementation(project(":nessie-versioned-storage-cassandra2-tests"))
  implementation(project(":nessie-versioned-storage-dynamodb-tests"))
  implementation(project(":nessie-versioned-storage-dynamodb2-tests"))
  implementation(project(":nessie-versioned-storage-jdbc-tests"))
  implementation(project(":nessie-versioned-storage-jdbc2-tests"))
  implementation(project(":nessie-versioned-storage-mongodb-tests"))
  implementation(project(":nessie-versioned-storage-mongodb2-tests"))
  implementation(project(":nessie-versioned-storage-rocksdb-tests"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-container-spec-helper"))

  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-junit5")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
  implementation("org.testcontainers:testcontainers-cassandra")
  implementation("org.testcontainers:testcontainers-postgresql")
  implementation("org.testcontainers:testcontainers-mysql")
  implementation("org.testcontainers:testcontainers-mariadb")
  implementation("org.testcontainers:testcontainers-mongodb")
  implementation(libs.docker.java.api)
  compileOnly(project(":nessie-keycloak-testcontainer"))
  compileOnly(project(":nessie-nessie-testcontainer"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }

  implementation(quarkusExtension(project, "amazon-services"))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")

  implementation(libs.testcontainers.keycloak)
  implementation(libs.keycloak.admin.client)

  compileOnly(project(":nessie-immutables-std"))
}
