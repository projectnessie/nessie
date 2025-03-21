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

plugins { id("nessie-conventions-server") }

publishingHelper { mavenName = "Nessie - Storage - JDBC" }

description =
  "Storage implementation for JDBC, supports H2, PostgreSQL, CockroachDB, MariaDB and MySQL (via MariaDB Driver)."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-proto"))
  implementation(project(":nessie-versioned-storage-common-serialize"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.guava)
  implementation(libs.agrona)
  implementation(libs.slf4j.api)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testFixturesApi(project(":nessie-versioned-storage-jdbc-tests"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-common-tests"))
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  testFixturesApi(project(":nessie-versioned-tests"))
  testFixturesImplementation(libs.agroal.pool)
  testRuntimeOnly(libs.h2)
  intTestRuntimeOnly(libs.postgresql)
  intTestRuntimeOnly(libs.mariadb.java.client)
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly(libs.docker.java.api)
  testFixturesImplementation(platform(libs.junit.bom))
  testFixturesImplementation(libs.bundles.junit.testing)
  testFixturesImplementation(libs.logback.classic)
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}
