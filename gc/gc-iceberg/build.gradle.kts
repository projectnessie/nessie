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

extra["maven.name"] = "Nessie - GC - Iceberg content functionality"

dependencies {
  implementation(platform(nessieRootProject()))
  implementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation("org.apache.iceberg:iceberg-core")

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))

  implementation("org.slf4j:slf4j-api")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.google.code.findbugs:jsr305")

  testImplementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  testImplementation(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testAnnotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))

  testImplementation(nessieProject("nessie-gc-iceberg-mock"))
  testRuntimeOnly("ch.qos.logback:logback-classic")

  testImplementation("com.google.guava:guava")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.mockito:mockito-core")
  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
