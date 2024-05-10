/*
 * Copyright (C) 2024 Dremio
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
  alias(libs.plugins.jmh)
}

extra["maven.name"] = "Nessie - Catalog - Object I/O"

dependencies {
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-catalog-files-api"))

  compileOnly(project(":nessie-doc-generator-annotations"))

  implementation(libs.guava)
  implementation(libs.caffeine)
  implementation(libs.micrometer.core)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:sts")
  implementation("software.amazon.awssdk:regions")
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }

  implementation(platform(libs.google.cloud.storage.bom))
  implementation("com.google.cloud:google-cloud-storage")

  implementation(platform(libs.azuresdk.bom))
  implementation("com.azure:azure-storage-file-datalake")
  implementation("com.azure:azure-identity")

  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(project(":nessie-object-storage-mock"))

  testRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhImplementation(project(":nessie-object-storage-mock"))
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

tasks.named("processJmhJandexIndex").configure { enabled = false }

tasks.named("processTestJandexIndex").configure { enabled = false }

jmh { jmhVersion = libs.versions.jmh.get() }

tasks.named<Jar>("jmhJar") { manifest { attributes["Multi-Release"] = "true" } }
