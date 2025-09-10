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

publishingHelper { mavenName = "Nessie - Catalog - Service Common" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-catalog-service-config"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-catalog-service-transfer"))

  compileOnly(project(":nessie-doc-generator-annotations"))
  compileOnly(libs.smallrye.config.core)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jdk8")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  runtimeOnly("com.fasterxml.jackson.datatype:jackson-datatype-guava")

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi(libs.smallrye.config.core)

  testCompileOnly(project(":nessie-immutables"))
  testAnnotationProcessor(project(":nessie-immutables", configuration = "processor"))
}
