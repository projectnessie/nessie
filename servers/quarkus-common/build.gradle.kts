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
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Quarkus Common"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-store"))
  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-dynamodb"))
  implementation(project(":nessie-versioned-persist-mongodb"))
  implementation(project(":nessie-versioned-persist-transactional"))

  implementation(project(":nessie-versioned-storage-cache"))
  implementation(project(":nessie-versioned-storage-cassandra"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-jdbc"))
  implementation(project(":nessie-versioned-storage-mongodb"))
  implementation(project(":nessie-versioned-storage-rocksdb"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-telemetry"))

  implementation(enforcedPlatform(libs.quarkus.bom))
  implementation(enforcedPlatform(libs.quarkus.amazon.services.bom))
  implementation(enforcedPlatform(libs.quarkus.cassandra.bom))
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("io.quarkus:quarkus-agroal")
  implementation("io.quarkus:quarkus-jdbc-postgresql")
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  implementation(libs.awssdk.apache.client) { exclude("commons-logging", "commons-logging") }
  implementation("io.quarkus:quarkus-mongodb-client")
  implementation("com.datastax.oss.quarkus:cassandra-quarkus-client")
  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")
  implementation(libs.opentelemetry.api)

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly(libs.jackson.annotations)

  compileOnly(libs.microprofile.openapi)
}

buildForJava11()
