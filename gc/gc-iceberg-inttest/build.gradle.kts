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

publishingHelper { mavenName = "Nessie - GC - Integration tests" }

val sparkScala = useSparkScalaVersionsForProject("3.5", "2.12")

dependencies {
  implementation(libs.hadoop.client)

  implementation(platform(libs.iceberg.bom))

  // Enforce a single version of Netty among dependencies
  // (Spark, Hadoop and Azure)
  implementation(enforcedPlatform(libs.netty.bom))

  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")
  implementation("org.apache.iceberg:iceberg-gcp")
  implementation("org.apache.iceberg:iceberg-azure")

  compileOnly(libs.errorprone.annotations)
  compileOnly(libs.microprofile.openapi)

  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))
  implementation(nessieProject("nessie-gc-iceberg"))
  implementation(nessieProject("nessie-gc-iceberg-mock"))
  implementation(nessieProject("nessie-gc-iceberg-files"))
  implementation(nessieProject("nessie-object-storage-mock"))
  implementation(nessieProject("nessie-minio-testcontainer"))

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
  intTestImplementation(
    nessieProject("nessie-spark-extensions-base_${sparkScala.scalaMajorVersion}")
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

  intTestRuntimeOnly(platform(libs.iceberg.bom))
  intTestRuntimeOnly("org.apache.iceberg:iceberg-nessie")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-core")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-aws")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-gcp")
  intTestRuntimeOnly("org.apache.iceberg:iceberg-azure")
  // Reference the exact Iceberg version here, because the iceberg-bom might not contain all
  // Spark/Flink deps :(
  intTestRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )
  intTestRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )

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
  intTestRuntimeOnly(platform(libs.google.cloud.libraries.bom))
  intTestImplementation("com.google.cloud:google-cloud-storage")
  intTestRuntimeOnly("com.google.cloud:google-cloud-nio")
  intTestRuntimeOnly(libs.google.cloud.bigdataoss.gcs.connector)
  intTestRuntimeOnly(libs.google.cloud.bigdataoss.gcsio) {
    // brings junit:junit + hamcrest :(
    exclude("io.grpc", "grpc-testing")
  }

  intTestImplementation(nessieProject("nessie-azurite-testcontainer"))
  intTestImplementation(nessieProject("nessie-gcs-testcontainer"))
  intTestRuntimeOnly(libs.hadoop.azure)

  intTestCompileOnly(nessieProject("nessie-immutables-std"))
  intTestAnnotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

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

// Spark stuff is veeery sticky
tasks.named<Test>("intTest").configure { forkEvery = 1 }

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
