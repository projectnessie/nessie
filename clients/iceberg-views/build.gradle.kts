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
  id("org.projectnessie.nessie-project")
}

extra["maven.artifactId"] = "iceberg-views"

dependencies {
  implementation(platform(rootProject))
  implementation(projects.clients.client)
  implementation(projects.model)
  implementation("org.apache.iceberg:iceberg-api")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-common")
  implementation("org.apache.iceberg:iceberg-bundled-guava")
  implementation("org.apache.iceberg:iceberg-nessie") { exclude("org.projectnessie", "*") }
  implementation("org.apache.hadoop:hadoop-client") {
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("com.sun.jersey", "jersey-servlet")
  }
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation(platform(rootProject))
  testImplementation(projects.versioned.persist.persistTests)
  testImplementation(projects.versioned.persist.inmem)
  testImplementation(projects.servers.jaxRsTestextension)
  testImplementation("org.slf4j:log4j-over-slf4j")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
