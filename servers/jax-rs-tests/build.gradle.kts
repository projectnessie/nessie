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

extra["maven.name"] = "Nessie - REST-API Tests"

description = "Artifact for REST-API tests, includes Glassfish/Jersey/Weld implementation."

dependencies {
  implementation(platform(rootProject))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  api(platform(project(":nessie-deps-testing")))
  api(platform("org.junit:junit-bom"))

  implementation(project(":nessie-client"))
  implementation("com.google.guava:guava")
  api("io.rest-assured:rest-assured")
  implementation("com.google.code.findbugs:jsr305")

  api("org.assertj:assertj-core")
  api("org.junit.jupiter:junit-jupiter-api")
  api("org.junit.jupiter:junit-jupiter-params")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform(project(":nessie-deps-persist")))

  testImplementation(project(":nessie-jaxrs-testextension"))
  testImplementation("org.slf4j:jcl-over-slf4j")
  testRuntimeOnly("com.h2database:h2")

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") { maxParallelForks = Runtime.getRuntime().availableProcessors() }
