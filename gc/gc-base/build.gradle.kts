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

extra["maven.name"] = "Nessie - GC - Base Implementation"

val sparkScala = useSparkScalaVersionsForProject("3.2")

dependencies {
  implementation(platform(nessieRootProject()))
  annotationProcessor(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-build-only", gradle))
  compileOnly(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))

  forScala(sparkScala.scalaVersion)

  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  compileOnly(nessieProject("nessie-client"))
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  compileOnly("org.apache.iceberg:iceberg-api")
  compileOnly("org.apache.iceberg:iceberg-core")
  compileOnly("org.apache.iceberg:iceberg-nessie") { exclude("org.projectnessie") }
  compileOnly(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  compileOnly("org.apache.iceberg:iceberg-parquet")
  compileOnly("org.apache.parquet:parquet-column")

  testImplementation(nessieProjectPlatform("nessie-deps-iceberg", gradle))
  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testImplementation(platform("org.junit:junit-bom"))

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  testImplementation(nessieProject("nessie-jaxrs-testextension"))
  testImplementation(nessieProject("nessie-jaxrs-tests"))
  testImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
    exclude("com.sun.jersey", "jersey-servlet")
  }
  testImplementation("org.slf4j:log4j-over-slf4j")
  testImplementation("ch.qos.logback:logback-classic")

  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(project(":nessie-spark-extensions-base_${sparkScala.scalaMajorVersion}")) {
    testJarCapability()
  }
  testImplementation(
    project(
      ":nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )
  testImplementation("org.apache.iceberg:iceberg-nessie")
  testImplementation(
    "org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  testImplementation(
    "org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
  )
  testImplementation("org.apache.iceberg:iceberg-hive-metastore")

  testImplementation("org.assertj:assertj-core")
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

forceJava11ForTests()
