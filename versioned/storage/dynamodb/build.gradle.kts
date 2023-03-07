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

extra["maven.name"] = "Nessie - Storage - DynamoDB"

description = "Storage implementation for DynamoDB."

dependencies {
  implementation(project(":nessie-versioned-storage-common"))
  implementation(project(":nessie-versioned-storage-common-proto"))

  // javax/jakarta
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.javax.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  compileOnly(libs.errorprone.annotations)
  implementation(libs.agrona)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  implementation(libs.protobuf.java)
  implementation(platform(libs.awssdk.bom))
  implementation(libs.awssdk.dynamodb) { exclude("software.amazon.awssdk", "apache-client") }
  implementation(libs.awssdk.netty.nio.client)
  implementation(libs.awssdk.url.connection.client)

  compileOnly(libs.testcontainers.testcontainers)
  compileOnly(libs.docker.java.api)

  compileOnly(libs.immutables.builder)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  compileOnly(project(":nessie-versioned-storage-testextension"))

  testImplementation(project(":nessie-versioned-storage-common-tests"))
  testImplementation(project(":nessie-versioned-storage-testextension"))
  testImplementation(project(":nessie-versioned-tests"))
  testRuntimeOnly(libs.testcontainers.testcontainers)
  testRuntimeOnly(libs.docker.java.api)
  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)
}

// Testcontainers is not supported on Windows :(
if (Os.isFamily(Os.FAMILY_WINDOWS)) {
  tasks.withType<Test>().configureEach { this.enabled = false }
}

// Issue w/ testcontainers/podman in GH workflows :(
if (Os.isFamily(Os.FAMILY_MAC) && System.getenv("CI") != null) {
  tasks.named<Test>("intTest") { this.enabled = false }
}
