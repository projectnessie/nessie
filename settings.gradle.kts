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

val baseVersion = file("version.txt").readText().trim()

pluginManagement {
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }
}

gradle.beforeProject {
  version = baseVersion
  group = "org.projectnessie"
}

include("code-coverage")

fun nessieProject(name: String, directory: File): ProjectDescriptor {
  include(name)
  val p = project(":$name")
  p.name = name
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
    nessieProject(name as String, file(directory as String))
  }
}

loadProjects("gradle/projects.main.properties")

val ideSyncActive =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    gradle.startParameter.taskNames.any { it.startsWith("eclipse") }

// Needed when loading/syncing the whole integrations-tools-testing project with Nessie as an
// included build. IDEA gets here two times: the first run _does_ have the properties from the
// integrations-tools-testing build's `gradle.properties` file, while the 2nd invocation only runs
// from the included build.
if (gradle.parent != null && ideSyncActive) {
  val f = file("./build/additional-build.properties")
  if (f.isFile) {
    System.getProperties().putAll(loadProperties(f))
  }
}

// Cannot use isIntegrationsTestingEnabled() in buildSrc/src/main/kotlin/Utilities.kt, because
// settings.gradle is evaluated before buildSrc.
if (!System.getProperty("nessie.integrationsTesting.enable").toBoolean()) {
  loadProjects("gradle/projects.iceberg.properties")

  val sparkScala = loadProperties(file("clients/spark-scala.properties"))

  val sparkVersions = sparkScala["sparkVersions"].toString().split(",").map { it.trim() }
  val allScalaVersions = LinkedHashSet<String>()
  for (sparkVersion in sparkVersions) {
    val scalaVersions =
      sparkScala["sparkVersion-${sparkVersion}-scalaVersions"].toString().split(",").map {
        it.trim()
      }
    for (scalaVersion in scalaVersions) {
      allScalaVersions.add(scalaVersion)
      val artifactId = "nessie-spark-extensions-${sparkVersion}_$scalaVersion"
      nessieProject(artifactId, file("clients/spark-extensions/v${sparkVersion}")).buildFileName =
        "../build.gradle.kts"
      if (ideSyncActive) {
        break
      }
    }
  }

  for (scalaVersion in allScalaVersions) {
    nessieProject(
      "nessie-spark-extensions-base_$scalaVersion",
      file("clients/spark-extensions-base")
    )
    nessieProject(
      "nessie-spark-extensions-basetests_$scalaVersion",
      file("clients/spark-extensions-basetests")
    )
    if (ideSyncActive) {
      break
    }
  }

  if (!ideSyncActive) {
    nessieProject("nessie-spark-extensions", file("clients/spark-extensions/v3.1")).buildFileName =
      "../build.gradle.kts"
    nessieProject("nessie-spark-3.2-extensions", file("clients/spark-extensions/v3.2"))
      .buildFileName = "../build.gradle.kts"
    nessieProject("nessie-spark-extensions-base", file("clients/spark-extensions-base"))
  }
}

rootProject.name = "nessie"
