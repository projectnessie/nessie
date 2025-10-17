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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("com.gradleup.shadow")
  id("nessie-conventions-unpublished-tool")
  alias(libs.plugins.jmh)
}

publishingHelper { mavenName = "Nessie - Services - Microbenchmarks" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.slf4j.api)

  jmhRuntimeOnly(project(":nessie-server-store"))

  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(project(":nessie-versioned-storage-testextension"))

  // 'implementation' is necessary here, becasue of the `jmhCompileGeneratedClasses` task
  implementation(libs.microprofile.openapi)
  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  jmhImplementation(libs.jmh.core)
  jmhAnnotationProcessor(libs.jmh.generator.annprocess)
  jmhRuntimeOnly(project(":nessie-versioned-storage-inmemory"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-bigtable"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-cassandra"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-cassandra2"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-rocksdb"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-mongodb"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-mongodb2"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-dynamodb"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-dynamodb2"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-jdbc"))
  jmhRuntimeOnly(project(":nessie-versioned-storage-jdbc2"))
  jmhRuntimeOnly(platform(libs.testcontainers.bom))
  jmhRuntimeOnly("org.testcontainers:testcontainers")
  jmhRuntimeOnly("org.testcontainers:testcontainers-cassandra")
  jmhRuntimeOnly("org.testcontainers:testcontainers-mongodb")
  jmhRuntimeOnly("org.testcontainers:testcontainers-postgresql")
  jmhRuntimeOnly("org.testcontainers:testcontainers-cockroachdb")
  jmhRuntimeOnly(libs.docker.java.api)
  jmhRuntimeOnly(libs.agroal.pool)
  jmhRuntimeOnly(libs.h2)
  jmhRuntimeOnly(libs.postgresql)
  jmhRuntimeOnly(libs.logback.classic)
}

jmh { jmhVersion = libs.versions.jmh.get() }

tasks.named<ShadowJar>("jmhJar").configure { mergeServiceFiles() }
