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

import java.net.URI
import java.util.Properties

includeBuild("../build-logic") { name = "nessie-build-logic" }

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_11)) {
  throw GradleException("Build requires Java 11")
}

val baseVersion = file("../version.txt").readText().trim()

pluginManagement {
  repositories {
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
  }
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    mavenCentral()
    gradlePluginPortal()
    if (System.getProperty("withMavenLocal").toBoolean()) {
      mavenLocal()
    }
    maven {
      name = "Apache Snapshots"
      url = URI("https://repository.apache.org/content/repositories/snapshots/")
      mavenContent { snapshotsOnly() }
      metadataSources {
        // Workaround for
        // https://youtrack.jetbrains.com/issue/IDEA-327421/IJ-fails-to-import-Gradle-project-with-dependency-with-classifier
        ignoreGradleMetadataRedirection()
        mavenPom()
      }
    }
  }
}

val groupIdIntegrations = "org.projectnessie.nessie-integrations"
val projectPathToGroupId = mutableMapOf<String, String>()

projectPathToGroupId[":"] = groupIdIntegrations

gradle.beforeProject {
  version = baseVersion
  group = checkNotNull(projectPathToGroupId[path]) { "No groupId for project $path" }
}

fun nessieProject(name: String, groupId: String, directory: File): ProjectDescriptor {
  include(name)
  val p = project(":$name")
  p.projectDir = directory
  projectPathToGroupId[p.path] = groupId
  return p
}

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun loadProjects(file: String, groupId: String) =
  loadProperties(file(file)).forEach { name, directory ->
    nessieProject(name as String, groupId, file("../$directory"))
  }

val ideSyncActive =
  System.getProperty("idea.sync.active").toBoolean() ||
    System.getProperty("idea.active").toBoolean() ||
    System.getProperty("eclipse.product") != null ||
    gradle.startParameter.taskNames.any { it.startsWith("eclipse") }

loadProjects("../gradle/projects.iceberg.properties", groupIdIntegrations)

// Note: Unlike the "main" settings.gradle.kts this variant includes _all_ Spark _and_ Scala
// version variants in IntelliJ.

val sparkScala = loadProperties(file("../integrations/spark-scala.properties"))
val sparkVersions = sparkScala["sparkVersions"].toString().split(",").map { it.trim() }
val allScalaVersions = LinkedHashSet<String>()

for (sparkVersion in sparkVersions) {
  val scalaVersions =
    sparkScala["sparkVersion-${sparkVersion}-scalaVersions"].toString().split(",").map { it.trim() }
  for (scalaVersion in scalaVersions) {
    allScalaVersions.add(scalaVersion)
    val artifactId = "nessie-spark-extensions-${sparkVersion}_$scalaVersion"
    nessieProject(
        artifactId,
        groupIdIntegrations,
        file("../integrations/spark-extensions/v${sparkVersion}"),
      )
      .buildFileName = "../build.gradle.kts"
    if (ideSyncActive) {
      break
    }
  }
}

for (scalaVersion in allScalaVersions) {
  nessieProject(
    "nessie-spark-extensions-base_$scalaVersion",
    groupIdIntegrations,
    file("../integrations/spark-extensions-base"),
  )
  nessieProject(
    "nessie-spark-extensions-basetests_$scalaVersion",
    groupIdIntegrations,
    file("../integrations/spark-extensions-basetests"),
  )
  if (ideSyncActive) {
    break
  }
}

rootProject.name = "nessie-iceberg"
