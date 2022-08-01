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

extra["maven.name"] = "Nessie - Services"

dependencies {
  implementation(platform(rootProject))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(platform("org.projectnessie.cel:cel-bom"))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation(project(":nessie-model"))
  implementation(project(":nessie-versioned-spi"))
  implementation("org.slf4j:slf4j-api")
  implementation("org.projectnessie.cel:cel-tools")
  implementation("org.projectnessie.cel:cel-jackson")
  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")

  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
