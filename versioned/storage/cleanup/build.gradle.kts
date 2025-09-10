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

publishingHelper { mavenName = "Nessie - Storage - Cleanup unreferenced objects" }

description = "Identify and purge unreferenced objects in the Nessie repository."

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-transfer-related"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.microprofile.openapi)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)
  implementation(libs.agrona)
  implementation(libs.slf4j.api)

  compileOnly(project(":nessie-versioned-storage-testextension"))

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testImplementation(project(":nessie-versioned-storage-store"))
  testImplementation(project(":nessie-versioned-tests"))
  testImplementation(project(":nessie-server-store"))
  testImplementation(project(path = ":nessie-protobuf-relocated", configuration = "shadow"))
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(project(":nessie-immutables"))
  testAnnotationProcessor(project(":nessie-immutables", configuration = "processor"))

  testCompileOnly(libs.microprofile.openapi)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
}
