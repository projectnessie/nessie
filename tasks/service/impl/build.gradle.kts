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

extra["maven.name"] = "Nessie - Tasks - Service"

dependencies {
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-tasks-service-spi"))
  implementation(project(":nessie-tasks-service-async"))
  implementation(project(":nessie-versioned-storage-common"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.opentelemetry.bom))
  implementation("io.opentelemetry:opentelemetry-api")

  compileOnly(libs.vertx.core)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.threeten.extra)

  testImplementation(libs.vertx.core)

  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.jakarta.annotation.api)

  testCompileOnly(libs.immutables.builder)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testImplementation(platform(libs.jackson.bom))
  testImplementation("com.fasterxml.jackson.core:jackson-databind")
  testImplementation("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testRuntimeOnly(libs.logback.classic)

  jmhImplementation(libs.jmh.core)
  jmhImplementation(project(":nessie-versioned-storage-common-tests"))
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
}

tasks.named("processJmhJandexIndex").configure { enabled = false }

tasks.named("processTestJandexIndex").configure { enabled = false }

jmh { jmhVersion.set(libs.versions.jmh.get()) }
