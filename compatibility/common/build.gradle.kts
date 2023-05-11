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

extra["maven.name"] = "Nessie - Backward Compatibility - Common"

dependencies {
  api(project(":nessie-client"))
  api(project(":nessie-compatibility-jersey"))
  api(project(":nessie-multi-env-test-engine"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  compileOnly(project(":nessie-versioned-persist-mongodb-test"))

  implementation(platform(libs.jersey.bom))
  api(libs.slf4j.api)
  api(libs.logback.classic)
  implementation("org.apache.maven:maven-core:3.9.2")
  implementation(libs.maven.resolver.provider)
  implementation(libs.maven.resolver.connector.basic)
  implementation(libs.maven.resolver.transport.file)
  implementation(libs.maven.resolver.transport.http)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.javax.enterprise.cdi.api)

  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.annotations)

  implementation(platform(libs.junit.bom))
  api(libs.junit.jupiter.api)
  compileOnly(libs.junit.jupiter.engine)

  testImplementation(libs.mockito.core)
  testImplementation(libs.guava)
  testImplementation(project(":nessie-versioned-persist-non-transactional-test"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-persist-in-memory"))
  implementation(project(":nessie-versioned-persist-in-memory-test"))
  implementation(project(":nessie-versioned-persist-rocks"))
  implementation(project(":nessie-versioned-persist-rocks-test"))

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(libs.junit.platform.testkit)
  testImplementation(libs.junit.jupiter.params)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.junit.platform.launcher)
}

tasks.withType<Test>().configureEach {
  filter {
    // Exclude test-classes for the tests
    excludeTestsMatching("TestNessieCompatibilityExtensions\$*")
  }
}
