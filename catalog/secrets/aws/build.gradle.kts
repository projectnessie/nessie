/*
 * Copyright (C) 2024 Dremio
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

extra["maven.name"] = "Nessie - Catalog - Secrets AWS"

dependencies {
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(libs.guava)

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:apache-client") {
    exclude("commons-logging", "commons-logging")
  }
  implementation("software.amazon.awssdk:secretsmanager")

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))
  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  intTestCompileOnly(project(":nessie-immutables"))
  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers")
  intTestImplementation("org.testcontainers:localstack")
  intTestImplementation("org.testcontainers:junit-jupiter")
  intTestImplementation(project(":nessie-container-spec-helper"))
  intTestRuntimeOnly(libs.logback.classic)
}
