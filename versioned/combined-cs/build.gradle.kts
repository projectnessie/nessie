/*
 * Copyright (C) 2023 Dremio
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

publishingHelper { mavenName = "Nessie - Combined Client and Server" }

dependencies {
  api(project(":nessie-client"))
  api(project(":nessie-model"))
  implementation(project(":nessie-rest-services"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-server-store"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-inmemory"))
  implementation(project(":nessie-versioned-storage-store"))
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.hibernate.validator.cdi)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  compileOnly(libs.bundles.junit.testing)
  compileOnly(project(":nessie-client-testextension"))
  compileOnly("org.junit.jupiter:junit-jupiter-engine")

  testImplementation(project(":nessie-jaxrs-tests"))

  testRuntimeOnly(libs.h2)
  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.jakarta.annotation.api)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(libs.bundles.junit.testing)
  testImplementation(project(":nessie-client-testextension"))
  testCompileOnly("org.junit.jupiter:junit-jupiter-engine")
  testRuntimeOnly(libs.jakarta.ws.rs.api)
}
