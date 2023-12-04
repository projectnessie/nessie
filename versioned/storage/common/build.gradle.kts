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
  alias(libs.plugins.jmh)
}

extra["maven.name"] = "Nessie - Storage - Common"

description = "Storage interfaces and logic implementations."

dependencies {
  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(platform(libs.opentelemetry.instrumentation.bom.alpha))
  compileOnly("io.opentelemetry.instrumentation:opentelemetry-instrumentation-annotations")
  compileOnly(libs.micrometer.core)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(project(path = ":nessie-protobuf-relocated", configuration = "shadow"))
  implementation(libs.slf4j.api)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  implementation(libs.snappy.java)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")
  testImplementation("io.opentelemetry:opentelemetry-sdk-testing")

  testCompileOnly(libs.immutables.builder)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testImplementation(project(":nessie-versioned-storage-common-tests"))
  testImplementation(libs.threeten.extra)
  testRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhImplementation(project(":nessie-versioned-storage-common-tests"))
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

tasks.named("processJmhJandexIndex").configure { enabled = false }

tasks.named("processTestJandexIndex").configure { enabled = false }

jmh { jmhVersion.set(libs.versions.jmh.get()) }
