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
  id("nessie-conventions-iceberg")
  id("nessie-jacoco")
  alias(libs.plugins.nessie.run)
}

extra["maven.name"] = "Nessie - GC - Integration tests"

val sparkScala = useSparkScalaVersionsForProject("3.4", "2.12")

dependencies {
  implementation(libs.hadoop.client)
  implementation(libs.iceberg.core)
  implementation(libs.iceberg.aws)
  implementation(libs.iceberg.gcp)
  implementation(libs.iceberg.azure)

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))
  implementation(nessieProject("nessie-gc-iceberg"))
  implementation(nessieProject("nessie-gc-iceberg-mock"))
  implementation(nessieProject("nessie-gc-iceberg-files"))
  implementation(nessieProject("nessie-s3mock"))
  implementation(nessieProject("nessie-s3minio"))

  implementation(platform(libs.jackson.bom))

  implementation(libs.slf4j.api)

  implementation(libs.agrona)

  intTestImplementation(
    nessieProject("nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}")
  )
  intTestImplementation(
    nessieProject(
      "nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )

  intTestImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  intTestRuntimeOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }

  intTestRuntimeOnly(libs.iceberg.nessie)
  intTestRuntimeOnly(libs.iceberg.core)
  intTestRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )
  intTestRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )

  intTestRuntimeOnly(libs.iceberg.hive.metastore)
  intTestRuntimeOnly(libs.iceberg.aws)
  intTestRuntimeOnly(libs.iceberg.gcp)
  intTestRuntimeOnly(libs.iceberg.azure)

  intTestRuntimeOnly(libs.hadoop.client)
  intTestRuntimeOnly(libs.hadoop.aws)
  intTestRuntimeOnly("software.amazon.awssdk:sts")

  intTestImplementation(platform(libs.awssdk.bom))
  intTestImplementation("software.amazon.awssdk:s3")
  runtimeOnly("software.amazon.awssdk:url-connection-client")
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  intTestRuntimeOnly("software.amazon.awssdk:dynamodb")
  intTestRuntimeOnly("software.amazon.awssdk:glue")
  intTestRuntimeOnly("software.amazon.awssdk:kms")

  intTestImplementation(platform(libs.google.cloud.storage.bom))
  intTestImplementation("com.google.cloud:google-cloud-storage")
  intTestRuntimeOnly(libs.google.cloud.nio)
  intTestRuntimeOnly(libs.google.cloud.gcs.connector)

  intTestImplementation(nessieProject("nessie-azurite-testcontainer"))
  intTestRuntimeOnly(libs.hadoop.azure)

  intTestCompileOnly(libs.immutables.builder)
  intTestCompileOnly(libs.immutables.value.annotations)
  intTestAnnotationProcessor(libs.immutables.value.processor)

  intTestRuntimeOnly(libs.logback.classic)

  intTestCompileOnly(libs.jakarta.validation.api)
  intTestCompileOnly(libs.jakarta.annotation.api)

  intTestCompileOnly(libs.microprofile.openapi)

  intTestImplementation(platform(libs.junit.bom))
  intTestImplementation(libs.bundles.junit.testing)

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
  systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
}

forceJavaVersionForTests(sparkScala.runtimeJavaVersion)

tasks.withType(Test::class.java).configureEach {
  systemProperty("aws.region", "us-east-1")
  jvmArgumentProviders.add(
    CommandLineArgumentProvider {
      val tmpdir = project.layout.buildDirectory.get().asFile.resolve("tmpdir")
      tmpdir.mkdirs()
      listOf("-Djava.io.tmpdir=$tmpdir")
    }
  )
}
