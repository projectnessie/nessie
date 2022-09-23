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
  implementation(platform(rootProject))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(platform(project(":nessie-deps-testing")))
  implementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  compileOnly("com.google.errorprone:error_prone_annotations")

  implementation(platform("org.junit:junit-bom"))

  implementation("org.testcontainers:testcontainers")

  implementation("software.amazon.awssdk:s3")
  implementation("software.amazon.awssdk:url-connection-client")

  compileOnly("org.apache.hadoop:hadoop-common:${dependencyVersion("versionHadoop")}")

  implementation("org.junit.jupiter:junit-jupiter-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testImplementation("org.junit.jupiter:junit-jupiter-engine")
  testImplementation("org.apache.hadoop:hadoop-common:${dependencyVersion("versionHadoop")}")
}

tasks.withType(Test::class.java).configureEach { systemProperty("aws.region", "us-east-1") }
