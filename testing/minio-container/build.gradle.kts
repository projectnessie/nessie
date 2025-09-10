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

plugins { id("nessie-conventions-java11") }

publishingHelper { mavenName = "Nessie - Minio testcontainer" }

description = "JUnit extension providing a Minio instance."

dependencies {
  implementation(platform(libs.testcontainers.bom))
  implementation("org.testcontainers:testcontainers")
  implementation(project(":nessie-container-spec-helper"))
  compileOnly(project(":nessie-immutables-std"))

  implementation(platform(libs.awssdk.bom))
  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:url-connection-client")

  implementation(libs.guava)

  compileOnly(platform(libs.junit.bom))
  compileOnly("org.junit.jupiter:junit-jupiter-api")

  compileOnly(libs.errorprone.annotations)

  intTestImplementation(libs.bundles.junit.testing)
  intTestRuntimeOnly(libs.logback.classic)
}

tasks.withType(Test::class.java).configureEach { systemProperty("aws.region", "us-east-1") }
