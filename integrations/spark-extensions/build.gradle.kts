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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("nessie-conventions-spark")
  id("nessie-shadow-jar")
  id("nessie-license-report")
}

val sparkScala = getSparkScalaVersionsForProject()

val nessieQuarkusServer by configurations.creating

dependencies {
  // picks the right dependencies for scala compilation
  forScala(sparkScala.scalaVersion)

  implementation(nessieProject("nessie-cli-grammar"))
  implementation(project(":nessie-spark-extensions-base_${sparkScala.scalaMajorVersion}"))
  compileOnly("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }
  compileOnly("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }
  compileOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }
  implementation(nessieClientForIceberg())
  implementation(nessieProject("nessie-notice"))

  if (sparkScala.sparkMajorVersion == "3.3") {
    implementation(platform(libs.jackson.bom))
    testFixturesImplementation(platform(libs.jackson.bom))
  }

  testFixturesApi(project(":nessie-spark-extensions-basetests_${sparkScala.scalaMajorVersion}"))

  val versionIceberg = libs.versions.iceberg.get()
  testFixturesImplementation("org.apache.iceberg:iceberg-nessie:$versionIceberg")
  testFixturesImplementation("org.apache.iceberg:iceberg-spark-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg")
  testFixturesImplementation("org.apache.iceberg:iceberg-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}:$versionIceberg")
  testFixturesImplementation("org.apache.iceberg:iceberg-hive-metastore:$versionIceberg")
  testFixturesImplementation("org.apache.iceberg:iceberg-aws:$versionIceberg")
  testFixturesImplementation("org.apache.iceberg:iceberg-aws-bundle:$versionIceberg")
  intTestRuntimeOnly(libs.hadoop.client) {
    exclude("org.slf4j", "slf4j-reload4j")
  }
  intTestRuntimeOnly(libs.hadoop.aws)

  testFixturesRuntimeOnly(libs.logback.classic) {
    version { strictly(libs.versions.logback.compat.get()) }
    // Logback 1.3 brings Slf4j 2.0, which doesn't work with Spark up to 3.3
    exclude("org.slf4j", "slf4j-api")
  }
  testFixturesImplementation(libs.slf4j.log4j.over.slf4j) {
    version { require(libs.versions.slf4j.compat.get()) }
  }
  testFixturesImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }
  testFixturesImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }
  testFixturesImplementation("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") { forSpark(sparkScala.sparkVersion) }

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  testFixturesApi(nessieProject("nessie-object-storage-mock"))
  testFixturesApi(libs.nessie.runner.common)

  nessieQuarkusServer(nessieProject("nessie-quarkus", "quarkusRunner"))
}

forceJavaVersionForTests(sparkScala.runtimeJavaVersion)

tasks.named<ShadowJar>("shadowJar").configure {
  dependencies {
    include(dependency("org.projectnessie.nessie:.*:.*"))
    include(dependency("org.projectnessie.nessie-integrations:.*:.*"))
  }
}

tasks.named<Test>("intTest").configure {
  // Spark keeps a lot of stuff around in the JVM, breaking tests against different Iceberg catalogs, so give every test class its own JVM
  forkEvery = 1
  inputs.files(nessieQuarkusServer)
  val execJarProvider =
    configurations.named("nessieQuarkusServer").map { c ->
      val file = c.incoming.artifactView {}.files.first()
      listOf("-Dnessie.exec-jar=${file.absolutePath}")
    }
  val javaExec = javaToolchains.launcherFor {
    this.languageVersion.set(JavaLanguageVersion.of(21))
  }.map {
    "-Djava-exec=${it.executablePath}"
  }
  jvmArgumentProviders.add(CommandLineArgumentProvider { execJarProvider.get() + listOf(javaExec.get()) })
}
