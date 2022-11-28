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
  alias(libs.plugins.nessie.run)
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - CLI integration test"

val sparkScala = useSparkScalaVersionsForProject("3.3", "2.12")

dependencies {
  implementation(nessieProject("nessie-gc-tool", "shadow"))

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.immutables.value.annotations)
  annotationProcessor(libs.immutables.value.processor)

  // hadoop-common brings Jackson in ancient versions, pulling in the Jackson BOM to avoid that
  implementation(platform(libs.jackson.bom))
  implementation(libs.hadoop.common) {
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

  implementation(libs.slf4j.api)

  runtimeOnly(libs.h2)

  testRuntimeOnly(libs.logback.classic)

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

  testImplementation(libs.iceberg.core)
  testRuntimeOnly(libs.iceberg.hive.metastore)
  testRuntimeOnly(libs.iceberg.aws)
  testRuntimeOnly(libs.iceberg.nessie)
  testRuntimeOnly(libs.iceberg.core)
  testRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )
  testRuntimeOnly(libs.iceberg.hive.metastore)
  testRuntimeOnly(libs.iceberg.aws)

  testRuntimeOnly(libs.hadoop.client)
  testRuntimeOnly(libs.hadoop.aws)

  testRuntimeOnly(platform(libs.awssdk.bom))
  testRuntimeOnly(libs.awssdk.s3)
  testRuntimeOnly(libs.awssdk.url.connection.client)
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  testRuntimeOnly(libs.awssdk.dynamodb)
  testRuntimeOnly(libs.awssdk.glue)
  testRuntimeOnly(libs.awssdk.kms)

  testCompileOnly(libs.jackson.annotations)
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

val intTest = tasks.named<Test>("intTest")

intTest { systemProperty("aws.region", "us-east-1") }

nessieQuarkusApp {
  includeTask(intTest)
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
}

forceJava11ForTests()
