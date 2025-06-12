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
  id("nessie-conventions-unpublished-tool")
  alias(libs.plugins.nessie.run)
}

tasks.withType<JavaCompile>().configureEach { options.release = 11 }

publishingHelper { mavenName = "Nessie - GC - CLI integration test" }

val sparkScala = useSparkScalaVersionsForProject("3.4", "2.12")

dnsjavaDowngrade()

dependencies {
  implementation(nessieProject("nessie-gc-tool", "shadow"))

  compileOnly(libs.errorprone.annotations)
  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

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
  // Bump the jabx-impl version 2.2.3-1 via hadoop-common to make it work with Java 17+
  implementation(libs.jaxb.impl)

  implementation(libs.slf4j.api)

  runtimeOnly(libs.h2)

  intTestRuntimeOnly(libs.logback.classic)

  intTestImplementation(
    nessieProject("nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}")
  )
  intTestImplementation(nessieProject("nessie-minio-testcontainer"))

  intTestImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestRuntimeOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }

  intTestImplementation(platform(libs.iceberg.bom))
  intTestImplementation("org.apache.iceberg:iceberg-core")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-aws")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-gcp")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-azure")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-nessie")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-core")
  // Reference the exact Iceberg version here, because the iceberg-bom might not contain all
  // Spark/Flink deps :(
  intTestRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )

  intTestRuntimeOnly(libs.hadoop.client)
  intTestRuntimeOnly(libs.hadoop.aws)
  intTestRuntimeOnly("software.amazon.awssdk:sts")

  intTestRuntimeOnly(platform(libs.awssdk.bom))
  intTestRuntimeOnly("software.amazon.awssdk:s3")
  intTestRuntimeOnly("software.amazon.awssdk:url-connection-client")
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  intTestRuntimeOnly("software.amazon.awssdk:dynamodb")
  intTestRuntimeOnly("software.amazon.awssdk:glue")
  intTestRuntimeOnly("software.amazon.awssdk:kms")

  intTestRuntimeOnly(platform(libs.google.cloud.storage.bom))
  intTestRuntimeOnly(platform(libs.google.cloud.libraries.bom))
  intTestRuntimeOnly("com.google.cloud:google-cloud-storage")
  intTestRuntimeOnly("com.google.cloud:google-cloud-nio")
  intTestRuntimeOnly(libs.google.cloud.bigdataoss.gcs.connector)
  intTestRuntimeOnly(libs.google.cloud.bigdataoss.gcsio) {
    // brings junit:junit + hamcrest :(
    exclude("io.grpc", "grpc-testing")
  }

  intTestRuntimeOnly(platform(libs.azuresdk.bom))
  intTestRuntimeOnly("com.azure:azure-storage-file-datalake")
  intTestRuntimeOnly("com.azure:azure-identity")
  intTestRuntimeOnly(libs.hadoop.azure)

  intTestCompileOnly("com.fasterxml.jackson.core:jackson-annotations")
  intTestCompileOnly(libs.microprofile.openapi)

  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

val intTest = tasks.named<Test>("intTest")

intTest.configure {
  systemProperty("aws.region", "us-east-1")
  // Java 23 & Hadoop
  systemProperty("java.security.manager", "allow")
}

nessieQuarkusApp {
  includeTask(intTest)
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
  systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
}

forceJavaVersionForTests(sparkScala.runtimeJavaVersion)
