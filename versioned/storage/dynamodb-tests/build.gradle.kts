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

plugins { id("nessie-conventions-server") }

publishingHelper { mavenName = "Nessie - Storage - DynamoDB - Tests" }

description = "Base test code for creating test backends using DynamoDB."

dependencies {
  implementation(project(":nessie-versioned-storage-dynamodb"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-testextension"))
  implementation(project(":nessie-container-spec-helper"))

  compileOnly(libs.jakarta.annotation.api)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:dynamodb")
  implementation("software.amazon.awssdk:apache-client")

  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
}
