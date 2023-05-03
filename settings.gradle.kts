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

// Needed by NesQuEIT's manageNessieProjectDependency() in its settings.gradle.kts
System.setProperty("root-project.nessie-build", file(".").absolutePath)

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

plugins {
  id("com.gradle.enterprise") version ("3.13.1")
  if (System.getenv("CI") != null || System.getProperty("allow-java-download").toBoolean()) {
    // Enable automatic Java toolchain download in CI or when explicitly requested by the user.
    // If in doubt, install the required Java toolchain manually, preferably using a "proper"
    // package manager. The downside of letting Gradle automatically download toolchains is that
    // these will only get downloaded once, but not automatically updated.
    id("org.gradle.toolchains.foojay-resolver-convention") version ("0.5.0")
  }
}

gradleEnterprise {
  if (System.getenv("CI") != null) {
    buildScan {
      termsOfServiceUrl = "https://gradle.com/terms-of-service"
      termsOfServiceAgree = "yes"
      // Add some potentially interesting information from the environment
      listOf(
          "GITHUB_ACTION_REPOSITORY",
          "GITHUB_ACTOR",
          "GITHUB_BASE_REF",
          "GITHUB_HEAD_REF",
          "GITHUB_JOB",
          "GITHUB_REF",
          "GITHUB_REPOSITORY",
          "GITHUB_RUN_ID",
          "GITHUB_RUN_NUMBER",
          "GITHUB_SHA",
          "GITHUB_WORKFLOW"
        )
        .forEach { e ->
          val v = System.getenv(e)
          if (v != null) {
            value(e, v)
          }
        }
      val ghUrl = System.getenv("GITHUB_SERVER_URL")
      if (ghUrl != null) {
        val ghRepo = System.getenv("GITHUB_REPOSITORY")
        val ghRunId = System.getenv("GITHUB_RUN_ID")
        link("Summary", "$ghUrl/$ghRepo/actions/runs/$ghRunId")
        link("PRs", "$ghUrl/$ghRepo/pulls")
      }
    }
  }
}

val groupIdIntegrations = "org.projectnessie.nessie-integrations"
val groupIdMain = "org.projectnessie.nessie"
val projectPathToGroupId = mutableMapOf<String, String>()
val projectNameToGroupId = mutableMapOf<String, String>()

projectPathToGroupId[":"] = groupIdMain

val allLoadedProjects = mutableListOf<ProjectDescriptor>()

gradle.beforeProject {
  version = baseVersion
  group = checkNotNull(projectPathToGroupId[path]) { "No groupId for project $path" }

  if (path.startsWith(":pom-relocations")) {
    setupRelocationProject(this)
  }
}

fun nessieProject(name: String, groupId: String, directory: File): ProjectDescriptor {
  include(name)
  val p = project(":$name")
  p.name = name
  p.projectDir = directory
  projectPathToGroupId[p.path] = groupId
  allLoadedProjects.add(p)
  return p
}

fun loadProperties(file: File): Properties {
  val props = Properties()
  file.reader().use { reader -> props.load(reader) }
  return props
}

fun loadProjects(file: String, groupId: String) =
  loadProperties(file(file)).forEach { name, directory ->
    nessieProject(name as String, groupId, file(directory as String))
  }

loadProjects("gradle/projects.main.properties", groupIdMain)

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
  loadProjects("gradle/projects.iceberg.properties", groupIdIntegrations)

  val sparkScala = loadProperties(file("integrations/spark-scala.properties"))

  val noSourceChecksProjects = mutableSetOf<String>()

  val sparkVersions = sparkScala["sparkVersions"].toString().split(",").map { it.trim() }
  val allScalaVersions = mutableSetOf<String>()
  var first = true
  for (sparkVersion in sparkVersions) {
    val scalaVersions =
      sparkScala["sparkVersion-${sparkVersion}-scalaVersions"].toString().split(",").map {
        it.trim()
      }
    for (scalaVersion in scalaVersions) {
      allScalaVersions.add(scalaVersion)
      val artifactId = "nessie-spark-extensions-${sparkVersion}_$scalaVersion"
      nessieProject(
          artifactId,
          groupIdIntegrations,
          file("integrations/spark-extensions/v${sparkVersion}")
        )
        .buildFileName = "../build.gradle.kts"
      if (first) {
        first = false
      } else {
        noSourceChecksProjects.add(":$artifactId")
      }
      if (ideSyncActive) {
        break
      }
    }
  }

  first = true
  for (scalaVersion in allScalaVersions) {
    for (name in listOf("base", "basetests")) {
      val prj = "nessie-spark-extensions-${name}_$scalaVersion"
      nessieProject(prj, groupIdIntegrations, file("integrations/spark-extensions-${name}"))
      if (!first) {
        noSourceChecksProjects.add(":$prj")
      }
    }
    if (first) {
      first = false
    }
    if (ideSyncActive) {
      break
    }
  }

  projectPathToGroupId[":pom-relocations"] = "org.projectnessie"
  allLoadedProjects
    .filter { !it.name.startsWith("nessie-versioned-storage") }
    .forEach { projectDescriptor ->
      val projectDir = "pom-relocations/${projectDescriptor.name}"
      val projectPath = ":pom-relocations:${projectDescriptor.name}"
      include(projectPath)
      val p = project(projectPath)
      p.name = projectDescriptor.name
      p.projectDir = rootDir.resolve(projectDir)

      projectPathToGroupId[projectPath] = "org.projectnessie"
    }

  gradle.beforeProject {
    if (noSourceChecksProjects.contains(this.path)) {
      project.extra["duplicated-project-sources"] = true
    }
  }
}

/** Setup projects to create relocation-poms. */
fun setupRelocationProject(project: Project) =
  project.run {
    val newProjectPath = if (project.name == "pom-relocations") ":" else ":${project.name}"

    apply<MavenPublishPlugin>()
    apply<SigningPlugin>()
    configure<PublishingExtension> {
      publications {
        register<MavenPublication>("maven") {
          groupId = "org.projectnessie"
          if (project.name == "pom-relocations") {
            artifactId = "nessie"
          }
          version = project.version.toString()

          pom {
            withXml {
              asNode().appendNode("parent").run {
                appendNode("groupId", groupIdMain)
                appendNode("artifactId", "nessie")
                appendNode("version", project.version.toString())
              }
            }

            distributionManagement {
              relocation { groupId.set(projectPathToGroupId[newProjectPath]!!) }
            }
          }
        }
      }
    }

    if (project.hasProperty("release")) {
      configure<SigningExtension> {
        val signingKey: String? by project
        val signingPassword: String? by project
        useInMemoryPgpKeys(signingKey, signingPassword)
        val publishing = project.extensions.getByType(PublishingExtension::class.java)
        afterEvaluate { sign(publishing.publications.getByName("maven")) }
      }
    }
  }

rootProject.name = "nessie"
