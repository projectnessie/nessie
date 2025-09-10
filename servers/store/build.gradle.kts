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

publishingHelper { mavenName = "Nessie - Server - Store" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  api(project(":nessie-server-store-proto"))
  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.jakarta.validation.api)

  testImplementation(libs.guava)
  testCompileOnly(libs.microprofile.openapi)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}
