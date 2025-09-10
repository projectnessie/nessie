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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Storage - BigTable - Tests" }

description = "Base test code for creating test backends using BigTable."

dependencies {
  implementation(project(":nessie-versioned-storage-bigtable"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-container-spec-helper"))
  compileOnly(project(":nessie-immutables-std"))

  implementation(libs.slf4j.api)

  implementation(platform(libs.google.cloud.bigtable.bom))
  implementation("com.google.cloud:google-cloud-bigtable")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
}
