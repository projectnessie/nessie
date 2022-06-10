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
  `nessie-conventions`
}

extra["maven.artifactId"] = "nessie-quarkus-common"

extra["maven.name"] = "Nessie - Quarkus Common"

dependencies {
  implementation(platform(rootProject))
  implementation(projects.model)
  implementation(projects.servers.store)
  implementation(projects.servers.services)
  implementation(projects.versioned.spi)
  implementation(projects.versioned.persist.adapter)
  implementation(projects.versioned.persist.persistStore)
  implementation(projects.versioned.persist.inmem)
  implementation(projects.versioned.persist.nontx)
  implementation(projects.versioned.persist.rocks)
  implementation(projects.versioned.persist.dynamodb)
  implementation(projects.versioned.persist.mongodb)
  implementation(projects.versioned.persist.tx)
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation(enforcedPlatform("io.quarkus.platform:quarkus-amazon-services-bom"))
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

  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}
