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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Backward Compatibility - Tests" }

dependencies {
  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)

  implementation(project(":nessie-compatibility-common"))
  implementation(project(":nessie-client"))
  implementation(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")

  intTestImplementation(libs.guava)
  intTestImplementation(project(":nessie-versioned-storage-common"))
  intTestRuntimeOnly(project(":nessie-versioned-storage-rocksdb"))
  intTestRuntimeOnly(project(":nessie-versioned-storage-mongodb-tests"))
  intTestRuntimeOnly(project(":nessie-versioned-storage-testextension"))
  intTestRuntimeOnly(platform(libs.testcontainers.bom))
  intTestRuntimeOnly("org.testcontainers:testcontainers-mongodb")
  intTestRuntimeOnly(libs.mongodb.driver.sync)
}

tasks.withType<Test>().configureEach {
  systemProperty("junit.jupiter.extensions.autodetection.enabled", "true")
  systemProperty("nessie.internal.store-index-format-version", "1")
}

// Compatibility tests fail on macOS with the following message: `libc++abi: terminating
// with uncaught exception of type std::__1::system_error: mutex lock failed: Invalid argument`
//
// Compatibility tests fail, because Windows not supported by testcontainers (logged message)
if ((Os.isFamily(Os.FAMILY_MAC) || Os.isFamily(Os.FAMILY_WINDOWS)) && System.getenv("CI") != null) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}
