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
  id("nessie-conventions-java11")
  alias(libs.plugins.jmh)
}

publishingHelper { mavenName = "Nessie - Storage - Common" }

description = "Storage interfaces and logic implementations."

dependencies {
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations")
  compileOnly(libs.micrometer.core)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(project(path = ":nessie-protobuf-relocated", configuration = "shadow"))
  implementation(libs.slf4j.api)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.snappy.java)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")
  testImplementation("io.opentelemetry:opentelemetry-sdk-testing")

  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testImplementation(project(":nessie-versioned-storage-common-tests"))
  testImplementation(libs.threeten.extra)
  testRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhImplementation(project(":nessie-versioned-storage-common-tests"))
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

jmh { jmhVersion = libs.versions.jmh.get() }
