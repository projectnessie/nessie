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
  alias(libs.plugins.nessie.run)
  id("nessie-conventions-spark")
  id("nessie-jacoco")
  id("nessie-shadow-jar")
}

val sparkScala = useSparkScalaVersionsForProject("3.2")

dependencies {
  // picks the right dependencies for scala compilation
  forScala(sparkScala.scalaVersion)

  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-client"))
  implementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  implementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  implementation(libs.delta.core)
  compileOnly(libs.microprofile.openapi)
  compileOnly(libs.errorprone.annotations)

  testFixturesImplementation(nessieProject("nessie-model"))
  testFixturesImplementation(nessieProject("nessie-client"))
  testFixturesImplementation("org.apache.spark:spark-core_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testFixturesImplementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  testFixturesImplementation(
    nessieProject(
      "nessie-spark-extensions-${sparkScala.sparkMajorVersion}_${sparkScala.scalaMajorVersion}"
    )
  )
  testFixturesImplementation(
    "com.fasterxml.jackson.module:jackson-module-scala_${sparkScala.scalaMajorVersion}"
  )
  testFixturesImplementation(libs.delta.core)

  testFixturesImplementation(platform(libs.jackson.bom))
  testFixturesImplementation("com.fasterxml.jackson.core:jackson-databind")
  testFixturesApi(libs.microprofile.openapi)
  testFixturesImplementation(libs.logback.classic)
  testFixturesImplementation(libs.slf4j.log4j.over.slf4j)
  testFixturesCompileOnly(libs.errorprone.annotations)

  testFixturesApi(platform(libs.junit.bom))
  testFixturesApi(libs.bundles.junit.testing)

  nessieQuarkusServer(nessieQuarkusServerRunner())
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
  jvmArgumentsNonInput.add("-XX:SelfDestructTimer=30")
  systemProperties.put("nessie.server.send-stacktrace-to-client", "true")
}

forceJava11ForTests()

tasks.named<ShadowJar>("shadowJar") {
  dependencies {
    include(dependency("org.projectnessie.nessie:.*:.*"))
    include(dependency("org.projectnessie.nessie-integrations:.*:.*"))
  }
}
