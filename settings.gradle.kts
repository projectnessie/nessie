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
import java.time.Duration
import java.util.Properties
import org.gradle.api.configuration.BuildFeatures
import org.gradle.kotlin.dsl.support.serviceOf

includeBuild("build-logic") { name = "nessie-build-logic" }

val includedBuildsFile = file(".included-builds.properties")

if (includedBuildsFile.isFile) {
  val props = Properties()
  includedBuildsFile.reader().use {
    props.load(it)
  }
  props.forEach { n, path -> includeBuild(path) { name = n.toString() } }
}

if (!JavaVersion.current().isCompatibleWith(JavaVersion.VERSION_21)) {
  throw GradleException("Build requires Java 21")
}

val isCI = providers.environmentVariable("CI").isPresent
val ideaSyncActive = providers.systemProperty("idea.sync.active").map(String::toBoolean)
val ideaActive = providers.systemProperty("idea.active").map(String::toBoolean)
val eclipseProduct = providers.systemProperty("eclipse.product")
val configurationCacheRequested =
  gradle.serviceOf<BuildFeatures>().configurationCache.requested.getOrElse(false)

if (isCI && configurationCacheRequested) {
  throw GradleException(
    "Gradle configuration cache must not be enabled in CI because it can persist build configuration state to disk."
  )
}

val baseVersion = file("version.txt").readText().trim()

pluginManagement {
  repositories {
    if (providers.systemProperty("withMavenLocal").map(String::toBoolean).getOrElse(false)) {
      mavenLocal()
    }
    mavenCentral() // prefer Maven Central, in case Gradle's repo has issues
    gradlePluginPortal()
  }
}

dependencyResolutionManagement {
  repositoriesMode = RepositoriesMode.FAIL_ON_PROJECT_REPOS
  repositories {
    if (providers.systemProperty("withMavenLocal").map(String::toBoolean).getOrElse(false)) {
      mavenLocal()
    }
    mavenCentral()
    gradlePluginPortal()
    if (providers.systemProperty("withApacheSnapshots").map(String::toBoolean).getOrElse(false)) {
      // This is a hack to let Renovate _not_ query the Apache snapshot repository for all
      // dependencies.
      // See https://github.com/renovatebot/renovate/discussions/41291
      fun configureIndirectForRenovate(asfSnap: MavenArtifactRepository) {
        asfSnap.url = uri("https://repository.apache.org/content/repositories/snapshots/")
        asfSnap.mavenContent { snapshotsOnly() }
      }
      maven {
        name = "Apache Snapshots"
        configureIndirectForRenovate(this)
        metadataSources {
          // Workaround for
          // https://youtrack.jetbrains.com/issue/IDEA-327421/IJ-fails-to-import-Gradle-project-with-dependency-with-classifier
          ignoreGradleMetadataRedirection()
          mavenPom()
        }
      }
    }
    // Only used for nessie-events-ri
    maven {
      name = "Confluent"
      url = URI("https://packages.confluent.io/maven/")
      mavenContent { releasesOnly() }
    }
  }
}

plugins {
  id("com.gradle.develocity") version ("4.5.0")
  id("com.gradleup.nmcp.settings") version ("1.6.1")
  if (
    providers.environmentVariable("CI").isPresent ||
      providers.systemProperty("allow-java-download").map(String::toBoolean).getOrElse(false)
  ) {
    // Enable automatic Java toolchain download in CI or when explicitly requested by the user.
    // If in doubt, install the required Java toolchain manually, preferably using a "proper"
    // package manager. The downside of letting Gradle automatically download toolchains is that
    // these will only get downloaded once, but not automatically updated.
    id("org.gradle.toolchains.foojay-resolver-convention") version ("1.0.0")
  }
}

develocity {
  if (isCI) {
    buildScan {
      termsOfUseUrl = "https://gradle.com/terms-of-service"
      termsOfUseAgree = "yes"
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
          "GITHUB_WORKFLOW",
        )
        .forEach { e ->
          val v = providers.environmentVariable(e).orNull
          if (v != null) {
            value(e, v)
          }
        }
      val ghUrl = providers.environmentVariable("GITHUB_SERVER_URL").orNull
      if (ghUrl != null) {
        val ghRepo = providers.environmentVariable("GITHUB_REPOSITORY").orNull
        val ghRunId = providers.environmentVariable("GITHUB_RUN_ID").orNull
        link("Summary", "$ghUrl/$ghRepo/actions/runs/$ghRunId")
        link("PRs", "$ghUrl/$ghRepo/pulls")
      }
      buildScan { publishing { onlyIf { true } } }
    }
  } else {
    val isBuildScan = gradle.startParameter.isBuildScan
    buildScan { publishing { onlyIf { isBuildScan } } }
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
  ideaSyncActive.getOrElse(false) ||
    ideaActive.getOrElse(false) ||
    eclipseProduct.isPresent ||
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

// Cannot use isIncludedInNesQuEIT() in build-logic/src/main/kotlin/Utilities.kt, because
// settings.gradle is evaluated before build-logic, also the the parent's rootProject is not
// available here.
if (gradle.parent == null) {
  loadProjects("gradle/projects.iceberg.properties", groupIdIntegrations)

  val sparkScala = loadProperties(file("integrations/spark-scala.properties"))

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
          file("integrations/spark-extensions/v${sparkVersion}"),
        )
        .buildFileName = "../build.gradle.kts"
      if (first) {
        first = false
      }
      if (ideSyncActive) {
        break
      }
    }
  }

  for (scalaVersion in allScalaVersions) {
    for (name in listOf("base", "basetests")) {
      val prj = "nessie-spark-extensions-${name}_$scalaVersion"
      nessieProject(prj, groupIdIntegrations, file("integrations/spark-extensions-${name}"))
    }
    if (ideSyncActive) {
      break
    }
  }
}

rootProject.name = "nessie"

// Pass environment variables:
//    ORG_GRADLE_PROJECT_sonatypeUsername
//    ORG_GRADLE_PROJECT_sonatypePassword
// Gradle targets:
//    publishAggregationToCentralPortal
//    publishAggregationToCentralPortalSnapshots
//    (nmcpZipAggregation to just generate the single, aggregated deployment zip)
// Ref: Maven Central Publisher API:
//    https://central.sonatype.org/publish/publish-portal-api/#uploading-a-deployment-bundle
nmcpSettings {
  centralPortal {
    providers.environmentVariable("ORG_GRADLE_PROJECT_sonatypeUsername").orNull?.let(username::set)
    providers.environmentVariable("ORG_GRADLE_PROJECT_sonatypePassword").orNull?.let(password::set)
    publishingType.set(if (isCI) "AUTOMATIC" else "USER_MANAGED")
    publishingTimeout.set(Duration.ofMinutes(120))
    validationTimeout.set(Duration.ofMinutes(120))
    publicationName.set("nessie-$baseVersion")
  }
}
