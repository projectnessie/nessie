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

import java.time.Duration
import java.util.zip.ZipFile
import org.gradle.api.artifacts.repositories.MavenArtifactRepository
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.tasks.PublishToMavenRepository
import org.jetbrains.changelog.date
import org.jetbrains.gradle.ext.settings
import org.jetbrains.gradle.ext.taskTriggers

plugins {
  eclipse
  id("nessie-conventions-root")
  alias(libs.plugins.nmcp)
  alias(libs.plugins.jetbrains.changelog)
}

apply<ReleaseSupportPlugin>()

publishingHelper { mavenName = "Nessie" }

description = "Transactional Catalog for Data Lakes"

tasks.named<Wrapper>("wrapper").configure { distributionType = Wrapper.DistributionType.ALL }

// Pass environment variables:
//    ORG_GRADLE_PROJECT_sonatypeUsername
//    ORG_GRADLE_PROJECT_sonatypePassword
// Gradle targets:
//    publishAggregationToCentralPortal
//    publishAggregationToCentralPortalSnapshots
// Ref: Maven Central Publisher API:
//    https://central.sonatype.org/publish/publish-portal-api/#uploading-a-deployment-bundle
nmcpAggregation {
  centralPortal {
    username.value(providers.environmentVariable("ORG_GRADLE_PROJECT_sonatypeUsername"))
    password.value(providers.environmentVariable("ORG_GRADLE_PROJECT_sonatypePassword"))
    publishingType =
      if (providers.environmentVariable("CI").isPresent) "AUTOMATIC" else "USER_MANAGED"
    publishingTimeout = Duration.ofMinutes(120)
    validationTimeout = Duration.ofMinutes(120)
    publicationName = "${project.name}-$version"
  }
}

allprojects {
  pluginManager.withPlugin("maven-publish") {
    fun configureNmcpPublishing() {
      pluginManager.apply("com.gradleup.nmcp")

      extensions.configure<PublishingExtension> {
        repositories
          .withType<MavenArtifactRepository>()
          .matching { it.name == "nmcp" }
          .configureEach { url = uri(layout.buildDirectory.dir("nmcp/m2").get().asFile) }
      }
      tasks
        .withType<PublishToMavenRepository>()
        .matching { it.name.endsWith("ToNmcpRepository") }
        .configureEach {
          doFirst { repository.url = uri(layout.buildDirectory.dir("nmcp/m2").get().asFile) }
        }

      // Do not use NMCP's deprecated publishAllProjectsProbablyBreakingProjectIsolation() helper
      // here. Wire the producer configuration explicitly, so generated projects such as the Spark
      // extension variants cannot be lost through implicit variant selection.
      rootProject.dependencies.add(
        "nmcpAggregation",
        rootProject.dependencies.project(mapOf("path" to path, "configuration" to "nmcpProducer")),
      )
    }

    if (name.startsWith("nessie-spark-extensions")) {
      // Spark extension projects set a Scala-specific build directory in their build scripts.
      // Apply NMCP after that, otherwise the NMCP publish repository points to build/nmcp/m2
      // while the producer variant points to build/<scala>/nmcp/m2.
      afterEvaluate { configureNmcpPublishing() }
    } else {
      configureNmcpPublishing()
    }
  }
}

val requiredNmcpSparkArtifacts = providers.provider {
  val sparkScala = loadProperties(file("integrations/spark-scala.properties"))
  val sparkVersions = sparkScala["sparkVersions"].toString().split(",").map { it.trim() }
  val scalaVersions = linkedSetOf<String>()
  val artifacts = mutableListOf<String>()

  for (sparkVersion in sparkVersions) {
    sparkScala["sparkVersion-${sparkVersion}-scalaVersions"]
      .toString()
      .split(",")
      .map { it.trim() }
      .forEach { scalaVersion ->
        scalaVersions.add(scalaVersion)
        artifacts.add("nessie-spark-extensions-${sparkVersion}_$scalaVersion")
      }
  }

  scalaVersions.forEach { scalaVersion ->
    artifacts.add("nessie-spark-extensions-base_$scalaVersion")
  }

  artifacts
}

val checkNmcpAggregationSparkArtifacts =
  tasks.register("checkNmcpAggregationSparkArtifacts") {
    group = "Verification"
    description = "Checks that the NMCP aggregation zip contains all Spark extension artifacts."

    val aggregationZip = layout.buildDirectory.file("nmcp/zip/aggregation.zip")
    dependsOn("nmcpZipAggregation")
    inputs.file(aggregationZip)

    doLast {
      val version = project.version.toString()
      val entries =
        ZipFile(aggregationZip.get().asFile).use { zip ->
          zip.entries().asSequence().map { it.name }.toSet()
        }

      val missing =
        requiredNmcpSparkArtifacts.get().filter { artifactId ->
          val artifactDir = "org/projectnessie/nessie-integrations/$artifactId/$version/"
          fun isMainJar(entry: String): Boolean {
            val fileName = entry.substringAfterLast("/")
            val expectedReleaseFileName = "$artifactId-$version.jar"
            val expectedSnapshotFileName =
              Regex(
                Regex.escape("$artifactId-${version.removeSuffix("-SNAPSHOT")}-") +
                  """\d{8}\.\d{6}-\d+\.jar"""
              )

            return entry.startsWith(artifactDir) &&
              (fileName == expectedReleaseFileName ||
                (version.endsWith("-SNAPSHOT") && expectedSnapshotFileName.matches(fileName)))
          }

          entries.none { it.startsWith(artifactDir) && it.endsWith(".pom") } ||
            entries.none(::isMainJar)
        }

      check(missing.isEmpty()) {
        "NMCP aggregation zip is missing Spark artifacts for version $version: " +
          missing.joinToString(", ")
      }
    }
  }

tasks.named("nmcpPublishAggregationToCentralPortal") {
  dependsOn(checkNmcpAggregationSparkArtifacts)
}

val buildToolIntegrationGradle =
  tasks.register<Exec>("buildToolIntegrationGradle") {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Gradle, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("${project.projectDir}/gradlew", "-p", workingDir, "test")
  }

val buildToolIntegrationMaven =
  tasks.register<Exec>("buildToolIntegrationMaven") {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Maven, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("./mvnw", "--batch-mode", "clean", "test", "-Dnessie.version=${project.version}")
  }

val buildToolsIntegrationTest =
  tasks.register("buildToolsIntegrationTest") {
    group = "Verification"
    description =
      "Checks whether the bom works fine with build tools, requires preceding publishToMavenLocal in a separate Gradle invocation"

    dependsOn(buildToolIntegrationGradle)
    dependsOn(buildToolIntegrationMaven)
  }

val buildToolsIntegrationClean =
  tasks.register<Delete>("buildToolsIntegrationClean") {
    delete("build-tools-integration-tests/.gradle")
    delete("build-tools-integration-tests/build")
    delete("build-tools-integration-tests/target")
  }

val clean = tasks.named<Delete>("clean") { dependsOn(buildToolsIntegrationClean) }

publishingHelper {
  nessieRepoName = "nessie"
  inceptionYear = "2020"
}

spotless {
  kotlinGradle {
    // Must be repeated :( - there's no "addTarget" or so
    target(
      "nessie-iceberg/*.gradle.kts",
      "*.gradle.kts",
      "build-logic/*.gradle.kts",
      "build-logic/src/**/*.kt*",
    )
  }
}

changelog {
  repositoryUrl = "https://github.com/projectnessie/nessie"
  title = "Nessie Changelog"
  versionPrefix = "nessie-"
  header = provider { "${version.get()} Release (${date()})" }
  groups =
    listOf(
      "Highlights",
      "Upgrade notes",
      "Breaking changes",
      "New Features",
      "Changes",
      "Deprecations",
      "Fixes",
      "Commits",
    )
  version = provider { project.version.toString() }
}

idea.project.settings { taskTriggers { afterSync(":nessie-protobuf-relocated:jar") } }

copiedCodeChecks {
  addDefaultContentTypes()

  licenseFile = project.layout.projectDirectory.file("LICENSE")

  scanDirectories {
    register("build-logic") { srcDir("build-logic/src") }
    register("misc") {
      srcDir(".github")
      srcDir("codestyle")
      srcDir("design")
      srcDir("grafana")
    }
    register("gradle") {
      srcDir("gradle")
      exclude("wrapper/*.jar")
      exclude("wrapper/*.sha256")
    }
    register("helm") {
      srcDir("helm")
      exclude("nessie/LICENSE")
    }
    register("site") {
      srcDir("site")
      exclude("build/**")
      exclude(".cache/**")
      exclude("venv/**")
      exclude("in-dev/generated-docs")
    }
    register("root") {
      srcDir(".")
      include("*")
    }
    register("tools") {
      srcDir("tools")
      include("dockerbuild")
      include("tools/releases")
    }
  }
}

tasks.named<Wrapper>("wrapper") {
  actions.addLast {
    val script = scriptFile.readText()
    val scriptLines = script.lines().toMutableList()

    val insertAtLine =
      scriptLines.indexOf("# Use the maximum available, or set MAX_FD != -1 to use that value.")
    scriptLines.add(insertAtLine, "")
    scriptLines.add(insertAtLine, $$"[ -f \"${APP_HOME}/.env\" ] && . \"${APP_HOME}/.env\"")
    scriptLines.add(insertAtLine, $$". \"${APP_HOME}/gradle/gradlew-include.sh\"")

    scriptFile.writeText(scriptLines.joinToString("\n"))
  }
}
