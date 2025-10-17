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

publishingHelper { mavenName = "Nessie - GC - JDBC live-contents-set persistence" }

dependencies {
  compileOnly(libs.errorprone.annotations)
  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))
  compileOnly(libs.jetbrains.annotations)

  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))

  implementation(libs.guava)

  implementation(libs.agroal.pool)
  implementation(libs.postgresql)
  implementation(libs.mariadb.java.client)
  implementation(libs.h2)

  implementation(libs.slf4j.api)

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesApi(project(":nessie-gc-base"))
  testFixturesApi(project(":nessie-gc-base-tests"))

  testFixturesRuntimeOnly(libs.logback.classic)

  testFixturesApi(libs.guava)

  testFixturesApi(platform(libs.jackson.bom))
  testFixturesCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testFixturesCompileOnly(libs.microprofile.openapi)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  intTestImplementation(libs.postgresql)
  intTestImplementation(platform(libs.testcontainers.bom))
  intTestImplementation("org.testcontainers:testcontainers-postgresql")
  intTestImplementation("org.testcontainers:testcontainers-mariadb")
  intTestImplementation("org.testcontainers:testcontainers-mysql")
  intTestRuntimeOnly(libs.docker.java.api)
  intTestImplementation(project(":nessie-container-spec-helper"))
  intTestCompileOnly(project(":nessie-immutables-std"))
}
