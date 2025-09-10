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

publishingHelper { mavenName = "Nessie - REST-API Tests" }

description = "Artifact for REST-API tests, includes Glassfish/Jersey/Weld implementation."

dependencies {
  implementation(project(":nessie-client"))
  implementation(project(":nessie-client-testextension"))
  implementation(libs.guava)
  api(libs.rest.assured)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(libs.microprofile.openapi)

  api(libs.assertj.core)
  api(platform(libs.junit.bom))
  api("org.junit.jupiter:junit-jupiter-api")
  api("org.junit.jupiter:junit-jupiter-params")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":nessie-versioned-storage-inmemory-tests"))
  testImplementation(project(":nessie-versioned-storage-jdbc2-tests"))
  testRuntimeOnly(libs.agroal.pool)

  testImplementation(project(":nessie-jaxrs-testextension"))

  testImplementation(libs.slf4j.jcl.over.slf4j)
  testRuntimeOnly(libs.h2)
  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(libs.bundles.junit.testing)
}
