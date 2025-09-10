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

publishingHelper { mavenName = "Nessie - Storage - Apache Cassandra - Tests" }

description = "Base test code for creating test backends using Apache Cassandra."

dependencies {
  implementation(project(":nessie-versioned-storage-cassandra2"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-container-spec-helper"))

  compileOnly(libs.jakarta.annotation.api)

  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

  implementation(platform(libs.cassandra.driver.bom))
  implementation("org.apache.cassandra:java-driver-core")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:cassandra") {
    exclude("com.datastax.cassandra", "cassandra-driver-core")
  }
}
