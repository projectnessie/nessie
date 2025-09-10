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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Storage - Tests" }

description =
  "Base test code for 'Persist' and 'VersionStore' related tests used to test storage implementations."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-versioned-tests"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-model"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)

  implementation(libs.logback.classic)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  compileOnly(libs.microprofile.openapi)
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  // required for custom object serialization tests
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}
