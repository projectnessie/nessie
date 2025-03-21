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

publishingHelper { mavenName = "Nessie - Storage - MongoDB" }

description = "Storage implementation for MongoDB."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-proto"))
  implementation(project(":nessie-versioned-storage-common-serialize"))

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.mongodb.driver.sync)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  intTestImplementation(project(":nessie-versioned-storage-mongodb-tests"))
  intTestImplementation(project(":nessie-versioned-storage-common-tests"))
  intTestImplementation(project(":nessie-versioned-storage-testextension"))
  intTestImplementation(project(":nessie-versioned-tests"))
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly("org.testcontainers:mongodb")
  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)
  intTestRuntimeOnly(libs.logback.classic)
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest").configure { this.enabled = false }
}
