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
  id("org.projectnessie.nessie-project")
}

extra["maven.artifactId"] = "nessie-gc-base"

extra["maven.name"] = "Nessie - GC - Base Implementation"

val scalaVersion = dependencyVersion("versionScala2_12")

val sparkVersion = dependencyVersion("versionSpark31")

val clientNessieVersion = dependencyVersion("versionClientNessie")

dependencies {
  implementation(platform(rootProject))
  annotationProcessor(platform(rootProject))
  implementation(platform("com.fasterxml.jackson:jackson-bom"))

  forScala(scalaVersion)

  compileOnly("org.immutables:value-annotations")
  annotationProcessor("org.immutables:value-processor")

  compileOnly(projects.clients.client)
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  compileOnly("jakarta.validation:jakarta.validation-api")
  implementation("com.fasterxml.jackson.core:jackson-annotations")
  implementation("com.google.code.findbugs:jsr305")

  compileOnly("org.apache.spark:spark-sql_2.12") { forSpark(sparkVersion) }
  compileOnly("org.apache.iceberg:iceberg-api")
  compileOnly("org.apache.iceberg:iceberg-core")
  compileOnly("org.apache.iceberg:iceberg-nessie") { exclude("org.projectnessie") }
  compileOnly("org.apache.iceberg:iceberg-spark3")
  compileOnly("org.apache.iceberg:iceberg-parquet")
  compileOnly("org.apache.parquet:parquet-column")

  testImplementation(platform(rootProject))

  testCompileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  testImplementation(projects.servers.jaxRsTestextension)
  testImplementation(projects.servers.jaxRsTests)
  testImplementation("org.apache.spark:spark-sql_2.12") {
    forSpark(sparkVersion)
    exclude("com.sun.jersey", "jersey-servlet")
  }
  testImplementation("org.slf4j:log4j-over-slf4j")
  testImplementation("ch.qos.logback:logback-classic")

  testCompileOnly(platform("com.fasterxml.jackson:jackson-bom"))
  testCompileOnly("com.fasterxml.jackson.core:jackson-annotations")

  testImplementation(projects.clients.sparkExtensionsBase) { testJarCapability() }
  testImplementation(projects.clients.spark31Extensions)
  testImplementation("org.apache.iceberg:iceberg-nessie")
  testImplementation("org.apache.iceberg:iceberg-spark3")
  testImplementation("org.apache.iceberg:iceberg-spark3-extensions")
  testImplementation("org.apache.iceberg:iceberg-hive-metastore")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

forceJava11ForTests()
