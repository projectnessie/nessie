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
  id("nessie-conventions-server8")
  id("nessie-jacoco")
  alias(libs.plugins.annotations.stripper)
}

extra["maven.name"] = "Nessie - Services"

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(libs.slf4j.api)

  implementation(platform(libs.cel.bom))
  implementation("org.projectnessie.cel:cel-standalone")

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
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)

  testFixturesImplementation(libs.guava)

  testFixturesApi(project(":nessie-model"))
  testFixturesApi(project(":nessie-versioned-spi"))

  testFixturesApi(project(":nessie-versioned-persist-adapter"))
  testFixturesApi(project(":nessie-versioned-persist-store"))
  testFixturesApi(project(":nessie-versioned-persist-testextension"))

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesCompileOnly(libs.jakarta.annotation.api)
  testFixturesCompileOnly(libs.findbugs.jsr305)

  testRuntimeOnly(project(":nessie-server-store"))

  testImplementation(project(":nessie-versioned-persist-serialize"))
  testImplementation(project(":nessie-versioned-persist-in-memory"))
  testImplementation(project(":nessie-versioned-persist-in-memory-test"))
  intTestImplementation(project(":nessie-versioned-persist-rocks"))
  intTestImplementation(project(":nessie-versioned-persist-rocks-test"))
  intTestImplementation(project(":nessie-versioned-persist-dynamodb"))
  intTestImplementation(project(":nessie-versioned-persist-dynamodb-test"))
  intTestImplementation(project(":nessie-versioned-persist-mongodb"))
  intTestImplementation(project(":nessie-versioned-persist-mongodb-test"))
  testImplementation(project(":nessie-versioned-persist-transactional"))
  testImplementation(project(":nessie-versioned-persist-transactional-test"))

  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-store"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  testFixturesApi(project(":nessie-versioned-storage-inmemory"))
  testFixturesApi(project(":nessie-versioned-storage-jdbc"))
  testFixturesImplementation(libs.logback.classic)
  intTestImplementation(project(":nessie-versioned-storage-cassandra"))
  intTestImplementation(project(":nessie-versioned-storage-rocksdb"))
  intTestImplementation(project(":nessie-versioned-storage-mongodb"))
  intTestImplementation(project(":nessie-versioned-storage-dynamodb"))
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly("org.testcontainers:testcontainers")
  intTestRuntimeOnly("org.testcontainers:cassandra")
  intTestRuntimeOnly("org.testcontainers:mongodb")
  intTestRuntimeOnly(libs.docker.java.api)
  testRuntimeOnly(libs.agroal.pool)
  testRuntimeOnly(libs.h2)

  // javax/jakarta
  testCompileOnly(libs.jakarta.annotation.api)

  testFixturesCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.microprofile.openapi)

  testFixturesCompileOnly(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
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
