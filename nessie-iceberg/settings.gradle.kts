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

import java.util.Properties

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_11)) {
  throw GradleException("Build requires Java 11")
}

val baseVersion = file("../version.txt").readText().trim()

pluginManagement {
  // Cannot use a settings-script global variable/value, so pass the 'versions' Properties via
  // settings.extra around.
  val versions = java.util.Properties()
  val pluginIdPattern =
    java.util.regex.Pattern.compile("\\s*id\\(\"([^\"]+)\"\\) version \"([^\"]+)\"\\s*")
  settings.extra["nessieBuild.versions"] = versions

  versions["versionNessie"] = file("../version.txt").readText().trim()

  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }

  val versionErrorPronePlugin = "2.0.2"
  val versionIdeaExtPlugin = "1.1.6"
  val versionJandexPlugin = "1.82"
  val versionNessiePlugins = "0.2.12"
  val versionQuarkus = "2.13.0.Final"
  val versionShadowPlugin = "7.1.2"
  val versionSpotlessPlugin = "6.11.0"

  plugins {
    id("com.diffplug.spotless") version versionSpotlessPlugin
    id("com.github.johnrengelman.plugin-shadow") version versionShadowPlugin
    id("com.github.node-gradle.node") version "3.4.0"
    id("com.github.vlsi.jandex") version versionJandexPlugin
    id("io.gatling.gradle") version "3.8.4"
    id("io.github.gradle-nexus.publish-plugin") version "1.1.0"
    id("io.quarkus") version versionQuarkus
    id("me.champeau.jmh") version "0.6.8"
    id("net.ltgt.errorprone") version versionErrorPronePlugin
    id("org.jetbrains.gradle.plugin.idea-ext") version versionIdeaExtPlugin
    id("org.projectnessie") version "0.27.3"
    id("org.projectnessie.buildsupport.spotless") version versionNessiePlugins
    id("org.projectnessie.buildsupport.checkstyle") version versionNessiePlugins
    id("org.projectnessie.buildsupport.errorprone") version versionNessiePlugins
    id("org.projectnessie.buildsupport.ide-integration") version versionNessiePlugins
    id("org.projectnessie.buildsupport.jacoco") version versionNessiePlugins
    id("org.projectnessie.buildsupport.jacoco-aggregator") version versionNessiePlugins
    id("org.projectnessie.buildsupport.jandex") version versionNessiePlugins
    id("org.projectnessie.buildsupport.protobuf") version versionNessiePlugins
    id("org.projectnessie.buildsupport.publishing") version versionNessiePlugins
    id("org.projectnessie.buildsupport.reflectionconfig") version versionNessiePlugins
    id("org.projectnessie.smallrye-open-api") version versionNessiePlugins

    versions["versionQuarkus"] = versionQuarkus
    versions["versionErrorPronePlugin"] = versionErrorPronePlugin
    versions["versionIdeaExtPlugin"] = versionIdeaExtPlugin
    versions["versionSpotlessPlugin"] = versionSpotlessPlugin
    versions["versionJandexPlugin"] = versionJandexPlugin
    versions["versionShadowPlugin"] = versionShadowPlugin
    versions["versionNessieBuildPlugins"] = versionNessiePlugins

    // The project's settings.gradle.kts is "executed" before buildSrc's settings.gradle.kts and
    // build.gradle.kts.
    //
    // Plugin and important dependency versions are defined here and shared with buildSrc via
    // a properties file, and via an 'extra' property with all other modules of the Nessie build.
    //
    // This approach works fine with GitHub's dependabot as well
    val nessieBuildVersionsFile = file("build/nessieBuild/versions.properties")
    nessieBuildVersionsFile.parentFile.mkdirs()
    nessieBuildVersionsFile.outputStream().use {
      versions.store(it, "Nessie Build versions from settings.gradle.kts - DO NOT MODIFY!")
    }
  }
}

gradle.rootProject {
  val prj = this
  val versions = settings.extra["nessieBuild.versions"] as Properties
  versions.forEach { k, v -> prj.extra[k.toString()] = v }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.projectnessie"
}

fun nessieProject(name: String, directory: File): ProjectDescriptor {
  include(name)
  val p = project(":$name")
  p.projectDir = directory
  return p
}

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun loadProjects(file: String) {
  loadProperties(file(file)).forEach { name, directory ->
    nessieProject(name as String, file("../$directory"))
  }
}

val ideSyncActive =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    gradle.startParameter.taskNames.any { it.startsWith("eclipse") }

loadProjects("../gradle/projects.iceberg.properties")

// Note: Unlike the "main" settings.gradle.kts this variant includes _all_ Spark _and_ Scala
// version variants in IntelliJ.

val sparkScala = loadProperties(file("../clients/spark-scala.properties"))
val sparkVersions = sparkScala["sparkVersions"].toString().split(",").map { it.trim() }
val allScalaVersions = LinkedHashSet<String>()

for (sparkVersion in sparkVersions) {
  val scalaVersions =
    sparkScala["sparkVersion-${sparkVersion}-scalaVersions"].toString().split(",").map { it.trim() }
  for (scalaVersion in scalaVersions) {
    allScalaVersions.add(scalaVersion)
    val artifactId = "nessie-spark-extensions-${sparkVersion}_$scalaVersion"
    nessieProject(artifactId, file("../clients/spark-extensions/v${sparkVersion}")).buildFileName =
      "../build.gradle.kts"
    if (ideSyncActive) {
      break
    }
  }
}

for (scalaVersion in allScalaVersions) {
  nessieProject(
    "nessie-spark-extensions-base_$scalaVersion",
    file("../clients/spark-extensions-base")
  )
  nessieProject(
    "nessie-spark-extensions-basetests_$scalaVersion",
    file("../clients/spark-extensions-basetests")
  )
  if (ideSyncActive) {
    break
  }
}

if (!ideSyncActive) {
  nessieProject("nessie-spark-extensions", file("../clients/spark-extensions/v3.1")).buildFileName =
    "../build.gradle.kts"
  nessieProject("nessie-spark-3.2-extensions", file("../clients/spark-extensions/v3.2"))
    .buildFileName = "../build.gradle.kts"
  nessieProject("nessie-spark-extensions-base", file("../clients/spark-extensions-base"))
}

rootProject.name = "nessie-iceberg"
