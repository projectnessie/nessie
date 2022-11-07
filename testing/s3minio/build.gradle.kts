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
  `maven-publish`
  signing
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - Minio testcontainer"

description = "JUnit extension providing a Minio instance."

dependencies {
  implementation(libs.testcontainers.testcontainers)

  implementation(platform(libs.awssdk.bom))
  implementation(libs.awssdk.s3)
  implementation(libs.awssdk.url.connection.client)

  // hadoop-common brings Jackson in ancient versions, pulling in the Jackson BOM to avoid that
  implementation(platform(libs.jackson.bom))
  compileOnly(libs.hadoop.common)

  implementation(platform(libs.junit.bom))
  implementation(libs.junit.jupiter.api)

  compileOnly(libs.errorprone.annotations)

  testImplementation(libs.bundles.junit.testing)
  testImplementation(libs.junit.jupiter.engine)
  testImplementation(libs.hadoop.common) { withSparkExcludes() }

  testRuntimeOnly(libs.logback.classic)
}

tasks.withType(Test::class.java).configureEach { systemProperty("aws.region", "us-east-1") }
