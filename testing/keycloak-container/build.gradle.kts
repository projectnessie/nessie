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

publishingHelper { mavenName = "Nessie - Keycloak testcontainer" }

dependencies {
  implementation(libs.slf4j.api)
  implementation(project(":nessie-container-spec-helper"))
  compileOnly(project(":nessie-immutables-std"))
  api(libs.keycloak.admin.client)
  api(libs.testcontainers.keycloak) {
    exclude(group = "org.slf4j") // uses SLF4J 2.x, we are not ready yet
  }

  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)
  compileOnly(libs.errorprone.annotations)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))
}
