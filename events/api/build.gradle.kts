/*
 * Copyright (C) 2023 Dremio
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

extra["maven.name"] = "Nessie - Events - API"

dependencies {

  // Immutables
  implementation(libs.immutables.builder)
  implementation(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  // Jackson
  implementation(platform(libs.jackson.bom))
  implementation(libs.jackson.annotations)
  implementation(libs.jackson.databind)

  // ErrorProne
  implementation(libs.errorprone.annotations)

  // Testing
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.jackson.datatype.jdk8)
  testImplementation(libs.jackson.datatype.jsr310)
  testImplementation(libs.guava)
}
