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
  jacoco
  `maven-publish`
  scala
  id("com.github.johnrengelman.shadow")
  id("org.projectnessie")
  `nessie-conventions`
}

val scalaVersion = dependencyVersion("versionScala2_12")

val sparkVersion = dependencyVersion("versionSpark32")

dependencies {
  // picks the right dependencies for scala compilation
  forScala(scalaVersion)

  implementation(platform(nessieRootProject()))
  implementation(nessieProject("nessie-model"))
  implementation(nessieProject("nessie-client"))
  implementation("org.apache.spark:spark-core_2.12") { forSpark(sparkVersion) }
  implementation("org.apache.spark:spark-sql_2.12") { forSpark(sparkVersion) }
  implementation("io.delta:delta-core_2.12")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation(platform(nessieRootProject()))
  testImplementation(nessieProject("nessie-spark-3.2-extensions"))
  testImplementation("com.fasterxml.jackson.module:jackson-module-scala_2.12")
  testImplementation("com.fasterxml.jackson.core:jackson-databind")
  testImplementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")
  testImplementation("ch.qos.logback:logback-classic")
  testImplementation("org.slf4j:log4j-over-slf4j")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testImplementation("org.junit.jupiter:junit-jupiter-params")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")

  nessieQuarkusServer(nessieQuarkusServerRunner(this))
}

nessieQuarkusApp {
  includeTask(tasks.named<Test>("intTest"))
  environmentNonInput.put("HTTP_ACCESS_LOG_LEVEL", testLogLevel())
}

forceJava11ForTests()

tasks.named<ShadowJar>("shadowJar") {
  dependencies { include(dependency("org.projectnessie:.*:.*")) }
}
