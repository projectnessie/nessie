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

  plugins {

    // Note: this is NOT a real project but a hack for dependabot to manage the plugin versions.
    //
    // Background: dependabot only manages dependencies (incl Gradle plugins) in build.gradle[.kts]
    // files. It scans the root build.gradle[.kts] file and those in submodules referenced in
    // settings.gradle[.kts].
    // But dependabot does not manage managed plugin dependencies in settings.gradle[.kts].
    // However, since dependabot is a "dumb search and replace engine", we can use a trick:
    // 1. Have this "dummy" build.gradle.kts file with all managed plugin dependencies.
    // 2. Add an `include()` to this build file in settings.gradle.kts, surrounded with an
    //    `if (false)`, so Gradle does _not_ pick it up.
    // 3. Parse this file in our settings.gradle.kts, provide a `ResolutionStrategy` to the
    //    plugin dependencies.

    val pulledVersions =
      file("gradle/dependabot/build.gradle.kts")
        .readLines()
        .map { line -> pluginIdPattern.matcher(line) }
        .filter { matcher -> matcher.matches() }
        .associate { matcher -> matcher.group(1) to matcher.group(2) }

    resolutionStrategy {
      eachPlugin {
        if (requested.version == null) {
          var pluginId = requested.id.id
          // All Gradle plugins in https://github.com/projectnessie/gradle-build-plugins/ use
          // the same version, which is "managed" via the "org.projectnessie.buildsupport.spotless"
          // plugin.
          if (
            pluginId.startsWith("org.projectnessie.buildsupport.") ||
              pluginId == "org.projectnessie.smallrye-open-api"
          ) {
            pluginId = "org.projectnessie.buildsupport.spotless"
          }
          if (pulledVersions.containsKey(pluginId)) {
            useVersion(pulledVersions[pluginId])
          }
        }
      }
    }

    versions["versionQuarkus"] = pulledVersions["io.quarkus"]
    versions["versionErrorPronePlugin"] = pulledVersions["net.ltgt.errorprone"]
    versions["versionIdeaExtPlugin"] = pulledVersions["org.jetbrains.gradle.plugin.idea-ext"]
    versions["versionSpotlessPlugin"] = pulledVersions["com.diffplug.spotless"]
    versions["versionJandexPlugin"] = pulledVersions["com.github.vlsi.jandex"]
    versions["versionShadowPlugin"] = pulledVersions["com.github.johnrengelman.plugin-shadow"]
    versions["versionNessieBuildPlugins"] =
      pulledVersions["org.projectnessie.buildsupport.spotless"]

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

val ideaSyncActive = System.getProperty("idea.sync.active").toBoolean()

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
    if (ideaSyncActive) {
      break
    }
  }
}

for (scalaVersion in allScalaVersions) {
  nessieProject(
    "nessie-spark-extensions-base_$scalaVersion",
    file("../clients/spark-extensions-base")
  )
  if (ideaSyncActive) {
    break
  }
}

if (!ideaSyncActive) {
  nessieProject("nessie-spark-extensions", file("../clients/spark-extensions/v3.1")).buildFileName =
    "../build.gradle.kts"
  nessieProject("nessie-spark-3.2-extensions", file("../clients/spark-extensions/v3.2"))
    .buildFileName = "../build.gradle.kts"
  nessieProject("nessie-spark-extensions-base", file("../clients/spark-extensions-base"))
}

rootProject.name = "nessie-iceberg"
