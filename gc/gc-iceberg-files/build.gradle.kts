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

extra["maven.name"] = "Nessie - GC - Iceberg FileIO connector"

description =
  "Nessie GC integration tests with Spark, Iceberg and S3 as a separate project " +
    "due to the hugely different set of dependencies"

dependencies {
  implementation(platform(nessieRootProject()))
  implementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))

  implementation("org.apache.hadoop:hadoop-common") {
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("com.sun.jersey")
    exclude("org.eclipse.jetty")
    exclude("org.apache.zookeeper")
  }
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(nessieProject("nessie-gc-base"))

  implementation("org.slf4j:slf4j-api")

  testImplementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testAnnotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testImplementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  testImplementation(nessieProject("nessie-gc-base-tests"))
  testImplementation(nessieProject("nessie-s3mock"))
  testImplementation(nessieProject("nessie-s3minio"))

  testImplementation("org.apache.iceberg:iceberg-core")
  testRuntimeOnly("org.apache.iceberg:iceberg-aws")

  testRuntimeOnly("ch.qos.logback:logback-classic")

  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testRuntimeOnly("org.apache.hadoop:hadoop-aws")
  testImplementation("software.amazon.awssdk:s3")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.withType(Test::class.java).configureEach {
  systemProperty("aws.region", "us-east-1")
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      val tmpdir = project.buildDir.resolve("tmpdir")
      tmpdir.mkdirs()
      listOf("-Djava.io.tmpdir=$tmpdir")
    }
  )
}
