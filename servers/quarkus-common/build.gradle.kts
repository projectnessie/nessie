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
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-quarkus")))
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation(enforcedPlatform("io.quarkus.platform:quarkus-amazon-services-bom"))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

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
  implementation("io.quarkus:quarkus-hibernate-validator")
  implementation("jakarta.validation:jakarta.validation-api")
  implementation("com.google.protobuf:protobuf-java")
  implementation("org.jboss.slf4j:slf4j-jboss-logmanager")
  implementation("io.quarkus:quarkus-agroal")
  implementation("io.quarkus:quarkus-jdbc-postgresql")
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation("io.quarkus:quarkus-mongodb-client")

  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
}

preferJava11()
