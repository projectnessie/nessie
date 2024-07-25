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

plugins { id("nessie-conventions-quarkus") }

publishingHelper { mavenName = "Nessie - Quarkus Tests" }

// Need to use :nessie-model-jakarta instead of :nessie-model here, because Quarkus w/
// resteasy-reactive does not work well with multi-release jars, but as long as we support Java 8
// for clients, we have to live with :nessie-model producing an MR-jar. See
// https://github.com/quarkusio/quarkus/issues/40236 and
// https://github.com/projectnessie/nessie/issues/8390.
configurations.all { exclude(group = "org.projectnessie.nessie", module = "nessie-model") }

dependencies {
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-quarkus-config"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-storage-bigtable-tests"))
  implementation(project(":nessie-versioned-storage-cassandra-tests"))
  implementation(project(":nessie-versioned-storage-dynamodb-tests"))
  implementation(project(":nessie-versioned-storage-jdbc-tests"))
  implementation(project(":nessie-versioned-storage-mongodb-tests"))
  implementation(project(":nessie-versioned-storage-rocksdb-tests"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-container-spec-helper"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-junit5")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
  implementation("org.testcontainers:cassandra")
  implementation("org.testcontainers:postgresql")
  implementation("org.testcontainers:mysql")
  implementation("org.testcontainers:mariadb")
  implementation("org.testcontainers:mongodb")
  implementation(libs.docker.java.api)
  compileOnly(project(":nessie-keycloak-testcontainer"))
  compileOnly(project(":nessie-nessie-testcontainer"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }

  implementation(platform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")

  implementation(libs.testcontainers.keycloak)
  implementation(libs.keycloak.admin.client)

  compileOnly(libs.immutables.value.annotations)
}
