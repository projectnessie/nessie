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
  id("org.projectnessie")
  `nessie-conventions`
}

extra["maven.name"] = "Nessie - GC - Integration tests"

val sparkScala = useSparkScalaVersionsForProject("3.3", "2.12")

dependencies {
  implementation(platform(nessieRootProject()))
  implementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  implementation(platform("software.amazon.awssdk:bom:${dependencyVersion("versionAwssdk")}"))

  implementation("org.apache.hadoop:hadoop-client")
  implementation("org.apache.iceberg:iceberg-core")
  implementation("org.apache.iceberg:iceberg-aws")

  compileOnly("com.google.errorprone:error_prone_annotations")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  implementation(nessieProject("nessie-client"))
  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-gc-base"))
  implementation(nessieProject("nessie-gc-iceberg"))
  implementation(nessieProject("nessie-gc-iceberg-mock"))
  implementation(nessieProject("nessie-gc-iceberg-files"))
  implementation(nessieProject("nessie-s3mock"))
  implementation(nessieProject("nessie-s3minio"))

  implementation("org.slf4j:slf4j-api")

  implementation("org.agrona:agrona:${dependencyVersion("versionAgrona")}")

  testImplementation(nessieProjectPlatform("nessie-deps-testing", gradle))
  testCompileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testAnnotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  testImplementation(platform("org.junit:junit-bom"))

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

  testRuntimeOnly("org.apache.iceberg:iceberg-nessie")
  testRuntimeOnly("org.apache.iceberg:iceberg-core")
  testRuntimeOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  testRuntimeOnly("org.apache.iceberg:iceberg-hive-metastore")
  testRuntimeOnly("org.apache.iceberg:iceberg-aws")

  testRuntimeOnly("org.apache.hadoop:hadoop-client")
  testRuntimeOnly("org.apache.hadoop:hadoop-aws")

  testImplementation("software.amazon.awssdk:s3")
  testRuntimeOnly("software.amazon.awssdk:url-connection-client")
  // TODO those are needed, because Spark serializes some configuration stuff (Spark broadcast)
  testRuntimeOnly("software.amazon.awssdk:dynamodb")
  testRuntimeOnly("software.amazon.awssdk:glue")
  testRuntimeOnly("software.amazon.awssdk:kms")

  testCompileOnly("org.immutables:builder")
  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testRuntimeOnly("ch.qos.logback:logback-classic")
  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

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
