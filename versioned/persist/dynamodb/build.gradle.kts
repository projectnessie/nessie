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

import org.apache.tools.ant.taskdefs.condition.Os

plugins {
  `java-library`
  jacoco
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Versioned - Persist - DynamoDB"

dependencies {
  implementation(project(":nessie-versioned-persist-adapter"))
  implementation(project(":nessie-versioned-persist-non-transactional"))
  implementation(project(":nessie-versioned-persist-serialize"))
  implementation(project(":nessie-versioned-spi"))
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)
  implementation(libs.guava)

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  implementation(platform(libs.awssdk.bom))
  implementation(libs.awssdk.dynamodb) { exclude("software.amazon.awssdk", "apache-client") }
  implementation(libs.awssdk.netty.nio.client)
  implementation(libs.awssdk.url.connection.client)

  intTestImplementation(project(":nessie-model"))
  intTestImplementation(project(":nessie-versioned-tests"))
  intTestImplementation(project(":nessie-versioned-persist-testextension"))
  intTestImplementation(project(":nessie-versioned-persist-tests"))
  intTestImplementation(project(":nessie-versioned-persist-non-transactional-test"))
  intTestImplementation(project(":nessie-versioned-persist-dynamodb-test"))
  intTestImplementation(libs.logback.classic)

  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.named<Test>("intTest") { this.enabled = false }
}
