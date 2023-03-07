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

import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - Services"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.slf4j.api)

  implementation(platform(libs.cel.bom))
  implementation(libs.cel.tools)
  implementation(libs.cel.jackson)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(platform(libs.jackson.bom))
  compileOnly(libs.jackson.annotations)

  compileOnly(libs.microprofile.openapi)

  testRuntimeOnly(project(":nessie-server-store"))

  testImplementation(project(":nessie-versioned-persist-store"))
  testImplementation(project(":nessie-versioned-persist-adapter"))
  testImplementation(project(":nessie-versioned-persist-serialize"))
  testImplementation(project(":nessie-versioned-persist-testextension"))
  testImplementation(project(":nessie-versioned-persist-in-memory"))
  testImplementation(project(":nessie-versioned-persist-in-memory-test"))
  testImplementation(project(":nessie-versioned-persist-rocks"))
  testImplementation(project(":nessie-versioned-persist-rocks-test"))
  testImplementation(project(":nessie-versioned-persist-dynamodb"))
  testImplementation(project(":nessie-versioned-persist-dynamodb-test"))
  testImplementation(project(":nessie-versioned-persist-mongodb"))
  testImplementation(project(":nessie-versioned-persist-mongodb-test"))
  testImplementation(project(":nessie-versioned-persist-transactional"))
  testImplementation(project(":nessie-versioned-persist-transactional-test"))

  testImplementation(project(":nessie-versioned-storage-common"))
  testImplementation(project(":nessie-versioned-storage-store"))
  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-storage-inmemory"))
  testImplementation(project(":nessie-versioned-storage-jdbc"))
  testImplementation(project(":nessie-versioned-storage-cassandra"))
  testImplementation(project(":nessie-versioned-storage-rocksdb"))
  testImplementation(project(":nessie-versioned-storage-mongodb"))
  testImplementation(project(":nessie-versioned-storage-dynamodb"))
  testRuntimeOnly(libs.testcontainers.testcontainers)
  testRuntimeOnly(libs.testcontainers.cassandra)
  testRuntimeOnly(libs.testcontainers.mongodb)
  testRuntimeOnly(libs.docker.java.api)
  testRuntimeOnly(libs.agroal.pool)
  testRuntimeOnly(libs.h2)
  testRuntimeOnly(libs.postgresql)
  testRuntimeOnly(libs.testcontainers.postgresql)
  testRuntimeOnly(libs.testcontainers.cockroachdb)

  // javax/jakarta
  testCompileOnly(libs.jakarta.annotation.api)

  testCompileOnly(libs.microprofile.openapi)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly(libs.jackson.annotations)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

annotationStripper {
  registerDefault().configure {
    annotationsToDrop("^jakarta[.].+".toRegex())
    unmodifiedClassesForJavaVersion.set(11)
  }
}
