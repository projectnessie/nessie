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
  id("org.projectnessie.buildsupport.attach-test-jar")
}

extra["maven.name"] = "Nessie - Versioned Store SPI"

dependencies {
  implementation(platform(rootProject))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))

  implementation(project(":nessie-model"))
  implementation("com.google.protobuf:protobuf-java")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  compileOnly("org.immutables:builder")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.google.guava:guava")
  implementation("com.google.code.findbugs:jsr305")

  testImplementation(platform(project(":nessie-deps-testing")))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.mockito:mockito-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  // Need a few things from Quarkus, but don't leak the dependencies
  compileOnly("io.opentracing:opentracing-api:${dependencyVersion("versionOpentracing")}")
  compileOnly("io.opentracing:opentracing-util:${dependencyVersion("versionOpentracing")}")
  compileOnly("io.micrometer:micrometer-core:${dependencyVersion("versionMicrometer")}")
  testImplementation("io.opentracing:opentracing-api:${dependencyVersion("versionOpentracing")}")
  testImplementation("io.opentracing:opentracing-util:${dependencyVersion("versionOpentracing")}")
  testImplementation("io.micrometer:micrometer-core:${dependencyVersion("versionMicrometer")}")
}
