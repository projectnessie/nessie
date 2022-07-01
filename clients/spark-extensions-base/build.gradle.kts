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
  jacoco
  `maven-publish`
  scala
  `nessie-conventions`
  id("org.projectnessie.buildsupport.attach-test-jar")
}

val scalaVersion = dependencyVersion("versionScala2_12")

val sparkVersion = dependencyVersion("versionSpark31")

val clientNessieVersion = dependencyVersion("versionClientNessie")

dependencies {
  // picks the right dependencies for scala compilation
  forScala(scalaVersion)

  implementation(platform(rootProject))
  implementation(project(":nessie-spark-extensions-grammar"))
  compileOnly("org.apache.spark:spark-hive_2.12") { forSpark(sparkVersion) }
  implementation("org.projectnessie:nessie-client:$clientNessieVersion")
  compileOnly("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testImplementation(platform(rootProject))
  testAnnotationProcessor(platform(rootProject))
  testImplementation("org.apache.spark:spark-sql_2.12") { forSpark(sparkVersion) }
  testImplementation("org.eclipse.microprofile.openapi:microprofile-openapi-api")

  testCompileOnly("org.immutables:value-annotations")
  testAnnotationProcessor("org.immutables:value-processor")

  testImplementation("org.assertj:assertj-core")
  testImplementation(platform("org.junit:junit-bom"))
  testImplementation("org.junit.jupiter:junit-jupiter-api")
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}
