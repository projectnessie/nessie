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
  id("nessie-conventions-server")
  id("nessie-jacoco")
}

extra["maven.name"] = "Nessie - Storage - Cache"

description = "Provides caching functionality, bounded by heap pressure."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-serialize"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.errorprone.annotations)

  implementation(libs.guava)
  implementation(libs.caffeine)
  implementation(libs.micrometer.core)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  testImplementation(project(":nessie-versioned-storage-common-tests"))
  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))

  testCompileOnly(libs.immutables.builder)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.core:jackson-annotations")
  testImplementation("com.fasterxml.jackson.core:jackson-databind")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.logback.classic)
}
