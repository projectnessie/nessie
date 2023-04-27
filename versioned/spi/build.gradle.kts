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

extra["maven.name"] = "Nessie - Versioned Store SPI"

dependencies {
  implementation(project(":nessie-model"))
  api(project(path = ":nessie-protobuf-relocated", configuration = "shadow"))
  implementation(libs.jackson.databind)
  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  compileOnly(libs.microprofile.openapi)

  implementation(platform(libs.jackson.bom))
  compileOnly(libs.jackson.annotations)

  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)

  testCompileOnly(platform(libs.jackson.bom))
  testCompileOnly(libs.jackson.annotations)

  testCompileOnly(libs.microprofile.openapi)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)
  testCompileOnly(libs.jakarta.ws.rs.api)
  testCompileOnly(libs.javax.ws.rs)
  testCompileOnly(libs.jakarta.validation.api)
  testCompileOnly(libs.javax.validation.api)
  testCompileOnly(libs.jakarta.annotation.api)
  testCompileOnly(libs.findbugs.jsr305)

  // Need a few things from Quarkus, but don't leak the dependencies
  compileOnly(libs.opentracing.api)
  compileOnly(libs.opentracing.util)
  compileOnly(libs.micrometer.core)
  testImplementation(libs.opentracing.api)
  testImplementation(libs.opentracing.util)
  testImplementation(libs.micrometer.core)
}
