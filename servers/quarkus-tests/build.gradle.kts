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
  `java-library`
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Quarkus Tests"

dependencies {
  implementation(project(":nessie-quarkus-common"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-testextension"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-dynamodb-test"))
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-mongodb-test"))
  implementation(project(":nessie-versioned-persist-transactional"))
  implementation(project(":nessie-versioned-persist-transactional-test"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-testextension"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation("io.quarkus:quarkus-junit5")

  implementation(libs.testcontainers.testcontainers)
  implementation(libs.testcontainers.cassandra)
  implementation(libs.testcontainers.postgresql)
  implementation(libs.testcontainers.mongodb)
  implementation(libs.docker.java.api)

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }

  implementation(platform(libs.quarkus.amazon.services.bom))
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")

  implementation(libs.testcontainers.keycloak) {
    exclude(group = "org.slf4j") // uses SLF4J 2.x, we are not ready yet
  }
}

buildForJava11()
