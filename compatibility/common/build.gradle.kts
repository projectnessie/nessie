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

extra["maven.name"] = "Nessie - Backward Compatibility - Common"

dependencies {
  implementation(platform(rootProject))

  api(projects.clients.client)
  api(projects.compatibility.jersey)
  implementation(projects.servers.services)
  implementation(projects.versioned.persist.adapter)

  implementation(platform("org.glassfish.jersey:jersey-bom"))
  api("org.slf4j:slf4j-api")
  api("ch.qos.logback:logback-classic")
  implementation("org.apache.maven:maven-resolver-provider")
  implementation("org.apache.maven.resolver:maven-resolver-connector-basic")
  implementation("org.apache.maven.resolver:maven-resolver-transport-file")
  implementation("org.apache.maven.resolver:maven-resolver-transport-http")
  implementation("com.google.guava:guava")
  implementation("jakarta.enterprise:jakarta.enterprise.cdi-api")
  implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(platform("org.junit:junit-bom"))
  api("org.junit.jupiter:junit-jupiter-api")
  implementation("org.junit.jupiter:junit-jupiter-engine")
  implementation("org.junit.platform:junit-platform-launcher")

  testImplementation(platform(rootProject))
  testImplementation("org.mockito:mockito-core")
  testImplementation("com.google.guava:guava")
  testImplementation(projects.versioned.persist.inmem)
  testImplementation(projects.versioned.persist.inmem) { testJarCapability() }
  testImplementation(projects.versioned.persist.rocks)
  testImplementation(projects.versioned.persist.rocks) { testJarCapability() }
  compileOnly(projects.versioned.persist.mongodb) { testJarCapability() }

  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.platform:junit-platform-testkit")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
}

tasks.withType<Test>().configureEach {
  systemProperty("rocksdb.version", dependencyVersion("versionRocksDb"))
  filter {
    // Exclude test-classes for the tests
    excludeTestsMatching("TestNessieCompatibilityExtensions\$*")
  }
}
