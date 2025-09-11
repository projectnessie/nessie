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

publishingHelper { mavenName = "Nessie - Catalog - REST Service" }

description = "Nessie Catalog service implementation providing REST endpoints."

dependencies {
  implementation(project(":nessie-rest-common"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-files-impl"))
  implementation(project(":nessie-catalog-format-iceberg"))
  implementation(project(":nessie-catalog-model"))
  implementation(project(":nessie-catalog-service-common"))
  implementation(project(":nessie-catalog-service-config"))
  implementation(project(":nessie-catalog-files-api"))
  implementation(project(":nessie-catalog-secrets-api"))
  implementation(project(":nessie-model"))
  implementation(project(":nessie-services-config"))
  implementation(project(":nessie-services"))
  implementation(project(":nessie-versioned-spi"))
  compileOnly(libs.smallrye.config.core)

  compileOnly(project(":nessie-immutables"))
  annotationProcessor(project(":nessie-immutables", configuration = "processor"))

  implementation(libs.smallrye.mutiny)

  implementation(quarkusPlatform(project))
  implementation("io.quarkus:quarkus-rest-common")
  implementation("io.quarkus:quarkus-rest")

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-smile")

  implementation(libs.slf4j.api)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.enterprise.cdi.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:s3")
}
