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
  `nessie-conventions`
}

extra["maven.artifactId"] = "nessie-services"

extra["maven.name"] = "Nessie - Services"

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(projects.model)
  implementation(projects.versioned.spi)
  implementation("org.slf4j:slf4j-api")
  implementation(platform("org.projectnessie.cel:cel-bom"))
  implementation("org.projectnessie.cel:cel-tools")
  implementation("org.projectnessie.cel:cel-jackson")
  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")

  testImplementation(platform(rootProject))
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
