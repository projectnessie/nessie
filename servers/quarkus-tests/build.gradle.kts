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

extra["maven.artifactId"] = "nessie-quarkus-tests"

extra["maven.name"] = "Nessie - Quarkus Tests"

dependencies {
  implementation(platform(rootProject))
  implementation(projects.servers.quarkusCommon)
  implementation(projects.versioned.tests)
  implementation(projects.versioned.persist.adapter)
  implementation(projects.versioned.persist.persistTests)
  implementation(projects.versioned.persist.dynamodb)
  implementation(projects.versioned.persist.dynamodb) { testJarCapability() }
  implementation(projects.versioned.persist.mongodb)
  implementation(projects.versioned.persist.mongodb) { testJarCapability() }
  implementation(projects.versioned.persist.tx)
  implementation(projects.versioned.persist.tx) { testJarCapability() }
  implementation(enforcedPlatform("io.quarkus:quarkus-bom"))
  implementation("io.quarkus:quarkus-junit5")
  implementation("org.testcontainers:testcontainers")
  implementation("org.testcontainers:postgresql")
  implementation("org.testcontainers:mongodb")
  implementation("com.github.docker-java:docker-java-api")
  implementation(platform("software.amazon.awssdk:bom"))
  implementation(platform("io.quarkus.platform:quarkus-amazon-services-bom"))
  implementation("software.amazon.awssdk:dynamodb") {
    exclude("software.amazon.awssdk", "apache-client")
  }
  implementation("io.quarkiverse.amazonservices:quarkus-amazon-dynamodb")
}

java {
  sourceCompatibility = JavaVersion.VERSION_11
  targetCompatibility = JavaVersion.VERSION_11
}
