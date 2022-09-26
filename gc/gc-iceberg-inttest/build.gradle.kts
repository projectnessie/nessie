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
  alias(libs.plugins.nessie.run)
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Integration tests"

val sparkScala = useSparkScalaVersionsForProject("3.3", "2.12")

dependencies {
  implementation(libs.hadoop.client)
  implementation(libs.iceberg.core)
  implementation(libs.iceberg.aws)

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

  implementation(libs.slf4j.api)

  implementation(libs.agrona)

  testImplementation(
    nessieProject("nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}")
  )
  testImplementation(
    nessieProject(
      "nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )

  testImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testRuntimeOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }

  testRuntimeOnly(libs.iceberg.nessie)
  testRuntimeOnly(libs.iceberg.core)
  testRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:${libs.versions.iceberg.get()}"
  )
  testRuntimeOnly(libs.iceberg.hive.metastore)
  testRuntimeOnly(libs.iceberg.aws)

  testRuntimeOnly(libs.hadoop.client)
  testRuntimeOnly(libs.hadoop.aws)

  testImplementation(platform(libs.awssdk.bom))
  testImplementation(libs.awssdk.s3)
  testRuntimeOnly(libs.awssdk.url.connection.client)
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  testRuntimeOnly(libs.awssdk.dynamodb)
  testRuntimeOnly(libs.awssdk.glue)
  testRuntimeOnly(libs.awssdk.kms)

  testCompileOnly(libs.immutables.builder)
  testCompileOnly(libs.immutables.value.annotations)
  testAnnotationProcessor(libs.immutables.value.processor)

  testRuntimeOnly(libs.logback.classic)
  testCompileOnly(libs.microprofile.openapi)

  testImplementation(platform(libs.junit.bom))
  testImplementation(libs.bundles.junit.testing)
  testRuntimeOnly(libs.junit.jupiter.engine)

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

forceJava11ForTests()

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
