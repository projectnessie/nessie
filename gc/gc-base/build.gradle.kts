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

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Base Implementation"

description =
  "Mark and sweep GC base functionality to identify live contents, map to live files, list existing files and to purge orphan files."

dependencies {
  implementation(platform(rootProject))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(project(":nessie-client"))
  implementation("org.slf4j:slf4j-api")
  implementation("com.google.guava:guava")
  implementation("org.agrona:agrona:${dependencyVersion("versionAgrona")}")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.google.code.findbugs:jsr305")

  testImplementation(platform(project(":nessie-deps-testing")))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(platform(project(":nessie-deps-build-only")))
  testAnnotationProcessor(platform(project(":nessie-deps-build-only")))

  testImplementation(project(":nessie-gc-base-tests"))
  testImplementation(project(":nessie-jaxrs-testextension"))

  testRuntimeOnly("ch.qos.logback:logback-classic")

  testCompileOnly("jakarta.validation:jakarta.validation-api")
  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testRuntimeOnly("ch.qos.logback:logback-classic")

  testImplementation("org.mockito:mockito-core")
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
