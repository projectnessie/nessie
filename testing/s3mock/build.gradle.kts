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

extra["maven.name"] = "Nessie - GC - S3 mock"

description = "Rudimentary S3 endpoint delegating to functions to serve content."

dependencies {
  implementation(platform(rootProject))
  implementation(platform(project(":nessie-deps-testing")))
  compileOnly(platform(project(":nessie-deps-build-only")))
  annotationProcessor(platform(project(":nessie-deps-build-only")))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))
  implementation(platform("org.glassfish.jersey:jersey-bom"))
  implementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  implementation("org.jboss.spec.javax.ws.rs:jboss-jaxrs-api_2.1_spec")
  implementation("javax.ws.rs:javax.ws.rs-api")
  implementation("org.glassfish.jersey.core:jersey-server")
  implementation("org.glassfish.jersey.containers:jersey-container-servlet")
  implementation("org.glassfish.jersey.containers:jersey-container-jetty-http")
  implementation("org.glassfish.jersey.inject:jersey-hk2")
  implementation("org.glassfish.jersey.media:jersey-media-json-jackson")

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation("com.google.guava:guava")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.fasterxml.jackson.core:jackson-databind")
  implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-xml")
  implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider")
  implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-xml-provider")
  compileOnly("org.apache.avro:avro:1.11.0")

  implementation("org.slf4j:slf4j-api")

  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  compileOnly("com.fasterxml.jackson.core:jackson-annotations")
  compileOnly("com.google.code.findbugs:jsr305")

  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(platform(project(":nessie-deps-build-only")))
  testAnnotationProcessor(platform(project(":nessie-deps-build-only")))

  testRuntimeOnly("ch.qos.logback:logback-classic")

  testImplementation("software.amazon.awssdk:s3")
  testImplementation("software.amazon.awssdk:url-connection-client")

  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testImplementation("org.junit.jupiter:junit-jupiter-engine")
}

tasks.withType(Test::class.java).configureEach { systemProperty("aws.region", "us-east-1") }
