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

plugins { id("nessie-conventions-spark") }

val sparkScala = getSparkScalaVersionsForProject()

dependencies {
  // picks the right dependencies for scala compilation
  forScala(sparkScala.scalaVersion)

  // javax/jakarta
  compileOnly(libs.jakarta.annotation.api)
  compileOnly(libs.findbugs.jsr305)

  implementation(nessieProject("nessie-cli-grammar"))
  compileOnly("org.apache.spark:spark-hive_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  implementation("org.apache.spark:spark-sql_${sparkScala.scalaMajorVersion}") {
    forSpark(sparkScala.sparkVersion)
  }
  implementation(nessieProject("nessie-spark-extensions-base_${sparkScala.scalaMajorVersion}"))

  implementation(nessieClientForIceberg())

  implementation(libs.microprofile.openapi)

  compileOnly(libs.errorprone.annotations)
  // This lets Immutables discover the right Guava version, otherwise we end with Guava 16 (from
  // 14), and Immutables uses Objects.toStringHelper instead of MoreObjects.toStringHelper.
  compileOnly(libs.guava)
  compileOnly(nessieProject("nessie-immutables-std"))
  annotationProcessor(nessieProject("nessie-immutables-std", configuration = "processor"))

  implementation(platform(libs.junit.bom))
  implementation(libs.bundles.junit.testing)
}
