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

publishingHelper { mavenName = "Nessie - Storage - Common serialization" }

description = "Serialization for storage objects."

dependencies {
  api(project(":nessie-versioned-storage-common"))
  api(project(":nessie-versioned-storage-common-proto"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")

  // required for custom object serialization tests
  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-guava")
  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  testImplementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

  testImplementation(project(":nessie-versioned-storage-common-tests"))

  testCompileOnly(project(":nessie-immutables-std"))
  testAnnotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}
