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

extra["maven.artifactId"] = "nessie-compatibility-tests"

extra["maven.name"] = "Nessie - Backward Compatibility - Tests"

dependencies {
  implementation(platform(rootProject))

  implementation(platform("org.junit:junit-bom"))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("org.assertj:assertj-core")
  implementation("org.junit.jupiter:junit-jupiter-api")
  implementation("org.junit.jupiter:junit-jupiter-params")
  implementation(projects.compatibility.common)
  implementation(projects.clients.client)
  implementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation("org.assertj:assertj-core")
  implementation(platform("org.junit:junit-bom"))
  implementation("org.junit.jupiter:junit-jupiter-api")

  testImplementation("com.google.guava:guava")
  testImplementation(projects.versioned.persist.adapter)
  testImplementation(projects.versioned.persist.nontx)
  testImplementation(projects.versioned.persist.inmem)
  testImplementation(projects.versioned.persist.inmem) { testJarCapability() }
  testImplementation(projects.versioned.persist.rocks)
  testImplementation(projects.versioned.persist.rocks) { testJarCapability() }
  testImplementation(projects.versioned.persist.mongodb)
  testImplementation(projects.versioned.persist.mongodb) { testJarCapability() }
}

tasks.withType<Test>().configureEach {
  systemProperty("rocksdb.version", dependencyVersion("versionRocksDb"))
  systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
}
