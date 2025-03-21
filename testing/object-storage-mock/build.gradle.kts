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

plugins { id("nessie-conventions-server") }

publishingHelper { mavenName = "Nessie - S3/ADLS/GCS object storage mock" }

description = "Rudimentary S3, ADLS-Gen2, GCS endpoint delegating to functions to serve content."

dependencies {
  compileOnly(libs.jakarta.ws.rs.api)
  compileOnly(libs.jakarta.validation.api)
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.jakarta.inject.api)

  compileOnly(libs.microprofile.openapi)

  compileOnly("io.quarkus:quarkus-arc:${libs.versions.quarkusPlatform.get()}")

  implementation(platform(libs.jersey.bom))
  implementation("org.glassfish.jersey.core:jersey-server")
  implementation("org.glassfish.jersey.containers:jersey-container-servlet")
  implementation("org.glassfish.jersey.containers:jersey-container-jetty-http")
  implementation("org.glassfish.jersey.inject:jersey-hk2")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson")

  compileOnly(libs.errorprone.annotations)
  compileOnly(project(":nessie-immutables-std"))
  annotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  implementation(libs.guava)

  implementation(platform(libs.jackson.bom))
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
  implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
  implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-json-provider")
  implementation("com.fasterxml.jackson.jakarta.rs:jackson-jakarta-rs-xml-provider")

  compileOnly(libs.avro)

  implementation(libs.slf4j.api)

  testRuntimeOnly(libs.logback.classic)

  testImplementation(platform(libs.awssdk.bom))
  testImplementation("software.amazon.awssdk:s3")
  testImplementation("software.amazon.awssdk:url-connection-client")
  testImplementation("software.amazon.awssdk:sts")

  testImplementation(platform(libs.azuresdk.bom))
  testImplementation("com.azure:azure-storage-file-datalake")
  testImplementation("com.azure:azure-identity")

  testImplementation(platform(libs.google.cloud.storage.bom))
  testImplementation("com.google.cloud:google-cloud-storage")

  testCompileOnly(project(":nessie-immutables-std"))
  testAnnotationProcessor(project(":nessie-immutables-std", configuration = "processor"))

  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
}

tasks.withType(Test::class.java).configureEach { systemProperty("aws.region", "us-east-1") }
