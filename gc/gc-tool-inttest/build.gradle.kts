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
  id("org.projectnessie")
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - CLI integration test"

val sparkScala = useSparkScalaVersionsForProject("3.3", "2.12")

dependencies {
  implementation(platform(nessieRootProject()))
  implementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  runtimeOnly(platform(project(":nessie-deps-persist")))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  implementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  implementation(nessieProject("nessie-gc-tool", "shadow"))

  implementation("org.apache.hadoop:hadoop-common") {
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.ws.rs", "javax.ws.rs-api")
    exclude("log4j", "log4j")
    exclude("org.slf4j", "slf4j-log4j12")
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("com.sun.jersey")
    exclude("org.eclipse.jetty")
    exclude("org.apache.hadoop")
    exclude("org.apache.zookeeper")
  }

  implementation("org.slf4j:slf4j-api")

  runtimeOnly("com.h2database:h2")

  testImplementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))
  testCompileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testAnnotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))

  testRuntimeOnly("ch.qos.logback:logback-classic:${dependencyVersion("versionLogback")}")

  testImplementation(
    nessieProject("nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}")
  )
  testImplementation(nessieProject("nessie-s3minio"))

  testImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testRuntimeOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }

  testImplementation("org.apache.iceberg:iceberg-core")
  testRuntimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  testRuntimeOnly("org.apache.iceberg:iceberg-aws")
  testRuntimeOnly("org.apache.iceberg:iceberg-nessie")
  testRuntimeOnly("org.apache.iceberg:iceberg-core")
  testRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  testRuntimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  testRuntimeOnly("org.apache.iceberg:iceberg-aws")

  testRuntimeOnly("org.apache.hadoop:hadoop-client")
  testRuntimeOnly("org.apache.hadoop:hadoop-aws")

  testRuntimeOnly("software.amazon.awssdk:s3")
  testRuntimeOnly("software.amazon.awssdk:url-connection-client")
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  testRuntimeOnly("software.amazon.awssdk:dynamodb")
  testRuntimeOnly("software.amazon.awssdk:glue")
  testRuntimeOnly("software.amazon.awssdk:kms")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

val intTest = tasks.named<Test>("intTest")

intTest { systemProperty("aws.region", "us-east-1") }

nessieQuarkusApp {
  includeTask(intTest)
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

forceJava11ForTests()
