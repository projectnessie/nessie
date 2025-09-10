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

publishingHelper { mavenName = "Nessie - Events - Service" }

dependencies {
  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation(project(":nessie-events-api"))
  implementation(project(":nessie-events-spi"))

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-core")
  implementation("com.fasterxml.jackson.core:jackson-databind")

  implementation(libs.slf4j.api)

  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.guava)
  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.jakarta.annotation.api)
}

tasks.withType(Test::class).configureEach {
  // Workaround when running tests with Java 21, can be removed once the mockito/bytebuddy issue is
  // fixed, see https://github.com/mockito/mockito/issues/3121
  systemProperty("net.bytebuddy.experimental", "true")
}
