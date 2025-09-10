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

publishingHelper { mavenName = "Nessie - GC - Base Implementation" }

description =
  "Mark and sweep GC base functionality to identify live contents, map to live files, list existing files and to purge orphan files."

dependencies {
  compileOnly(libs.errorprone.annotations)
  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  api(project(":nessie-storage-uri"))
  implementation(project(":nessie-client"))
  implementation(libs.slf4j.api)
  implementation(libs.guava)
  implementation(libs.agrona)
  implementation(libs.httpclient5)

  compileOnly(libs.microprofile.openapi)

  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)

  compileOnly(platform(libs.jackson.bom))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":nessie-gc-base-tests"))
  testImplementation(project(":nessie-jaxrs-testextension"))

  testImplementation(project(":nessie-versioned-storage-inmemory-tests"))

  testRuntimeOnly(libs.logback.classic)

  testCompileOnly(libs.microprofile.openapi)

  testCompileOnly(libs.jakarta.validation.api)

  testCompileOnly(project(":nessie-immutables-std"))
  testAnnotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

tasks.withType(Test::class.java).configureEach {
  // Java 23 & Hadoop
  systemProperty("java.security.manager", "allow")
}
