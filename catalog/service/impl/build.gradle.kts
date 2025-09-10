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

publishingHelper { mavenName = "Nessie - Catalog - Service Implementation" }

dependencies {
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-format-iceberg"))
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-config"))
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-tasks-api"))
  implementation(project(":nessie-tasks-service-async"))

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(libs.avro)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(platform(libs.jackson.bom))
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

  testCompileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)
  testFixturesApi(project(":nessie-versioned-storage-testextension"))
  testFixturesRuntimeOnly(project(":nessie-versioned-storage-inmemory"))
  testFixturesRuntimeOnly(libs.logback.classic)
  testFixturesApi(project(":nessie-tasks-service-impl"))
  testFixturesApi(project(":nessie-catalog-files-impl"))
  testFixturesApi(project(":nessie-catalog-format-iceberg-fixturegen"))
  testFixturesApi(project(":nessie-catalog-secrets-api"))
  testFixturesApi(project(":nessie-object-storage-mock"))
  testFixturesApi(project(":nessie-combined-cs"))
  testFixturesApi(project(":nessie-services"))
  testFixturesApi(project(":nessie-services-config"))
  testFixturesApi(project(":nessie-versioned-spi"))
  testFixturesApi(project(":nessie-versioned-storage-common"))
  testFixturesApi(project(":nessie-versioned-storage-store"))
  testFixturesApi(project(":nessie-rest-services"))
  testFixturesApi(libs.threeten.extra)
  testFixturesApi(libs.jakarta.ws.rs.api)

  testImplementation(testFixtures(project(":nessie-catalog-secrets-api")))
  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:s3")
  testImplementation("software.amazon.awssdk:url-connection-client")
  testCompileOnly(project(":nessie-immutables"))
  testAnnotationProcessor(project(":nessie-immutables", configuration = "processor"))
}
