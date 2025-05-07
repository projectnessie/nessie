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

import io.github.zenhelix.gradle.plugin.MavenCentralUploaderPlugin.Companion.MAVEN_CENTRAL_PORTAL_NAME
import io.github.zenhelix.gradle.plugin.extension.MavenCentralUploaderExtension
import io.github.zenhelix.gradle.plugin.extension.PublishingType
import io.github.zenhelix.gradle.plugin.task.PublishBundleMavenCentralTask
import io.github.zenhelix.gradle.plugin.task.ZipDeploymentTask
import java.time.Duration
import org.gradle.api.publish.plugins.PublishingPlugin.PUBLISH_TASK_GROUP
import org.gradle.kotlin.dsl.mavenCentralPortal
import org.jetbrains.changelog.date
import org.jetbrains.gradle.ext.settings
import org.jetbrains.gradle.ext.taskTriggers

plugins {
  eclipse
  id("nessie-conventions-root")
  alias(libs.plugins.maven.central.publish)
  alias(libs.plugins.jetbrains.changelog)
}

apply<ReleaseSupportPlugin>()

publishingHelper { mavenName = "Nessie" }

description = "Transactional Catalog for Data Lakes"

// To fix circular dependencies with NessieClient, certain projects need to use the same Nessie
// version as Iceberg/Delta has.
// Allow overriding the Iceberg version used by Nessie and the Nessie version used by integration
// tests that depend on Iceberg.
val versionIceberg: String =
  System.getProperty("nessie.versionIceberg", libs.versions.iceberg.get())
val versionClientNessie: String =
  System.getProperty("nessie.versionClientNessie", libs.versions.nessieClientVersion.get())

mapOf(
    "versionClientNessie" to versionClientNessie,
    "versionIceberg" to versionIceberg,
    "versionJandex" to libs.versions.jandex.get(),
  )
  .plus(loadProperties(file("integrations/spark-scala.properties")))
  .forEach { (k, v) -> extra[k.toString()] = v }

tasks.named<Wrapper>("wrapper").configure { distributionType = Wrapper.DistributionType.ALL }

// Pass environment variables:
//    ORG_GRADLE_PROJECT_sonatypeUsername
//    ORG_GRADLE_PROJECT_sonatypePassword
// Gradle targets:
//    publishAggregateMavenCentralDeployment
//    (zipAggregateMavenCentralDeployment to just generate the single, aggregated deployment zip)
// Ref: Maven Central Publisher API:
//    https://central.sonatype.org/publish/publish-portal-api/#uploading-a-deployment-bundle
mavenCentralPortal {
  credentials {
    username.value(provider { System.getenv("ORG_GRADLE_PROJECT_sonatypeUsername") })
    password.value(provider { System.getenv("ORG_GRADLE_PROJECT_sonatypePassword") })
  }

  deploymentName = "${project.name}-$version"

  // publishingType
  //   AUTOMATIC = fully automatic release
  //   USER_MANAGED = user has to manually publish/drop
  publishingType =
    if (System.getenv("CI") != null) PublishingType.AUTOMATIC else PublishingType.USER_MANAGED
  // baseUrl = "https://central.sonatype.com"
  uploader {
    // 2 seconds * 3600 = 7200 seconds = 2hrs
    delayRetriesStatusCheck = Duration.ofSeconds(2)
    maxRetriesStatusCheck = 3600

    aggregate {
      // Aggregate submodules into a single archive
      modules = true
      // Aggregate publications into a single archive for each module
      modulePublications = true
    }
  }
}

val mavenCentralDeploymentZipAggregation by configurations.creating

mavenCentralDeploymentZipAggregation.isTransitive = true

val zipAggregateMavenCentralDeployment by
  tasks.registering(Zip::class) {
    group = PUBLISH_TASK_GROUP
    description = "Generates the aggregated Maven publication zip file."

    inputs.files(mavenCentralDeploymentZipAggregation)
    from(mavenCentralDeploymentZipAggregation.map { zipTree(it) })
    // archiveFileName = mavenCentralPortal.deploymentName.orElse(project.name)
    destinationDirectory.set(layout.buildDirectory.dir("aggregatedDistribution"))
    doLast { logger.lifecycle("Built aggregated distribution ${archiveFile.get()}") }
  }

val publishAggregateMavenCentralDeployment by
  tasks.registering(PublishBundleMavenCentralTask::class) {
    group = PUBLISH_TASK_GROUP
    description =
      "Publishes the aggregated Maven publications $MAVEN_CENTRAL_PORTAL_NAME repository."

    val task = this

    dependsOn(zipAggregateMavenCentralDeployment)
    inputs.file(zipAggregateMavenCentralDeployment.flatMap { it.archiveFile })

    project.extensions.configure<MavenCentralUploaderExtension> {
      val ext = this
      task.baseUrl.set(ext.baseUrl)
      task.credentials.set(
        ext.credentials.username.flatMap { username ->
          ext.credentials.password.map { password ->
            io.github.zenhelix.gradle.plugin.client.model.Credentials.UsernamePasswordCredentials(
              username,
              password,
            )
          }
        }
      )

      task.publishingType.set(
        ext.publishingType.map {
          when (it) {
            PublishingType.AUTOMATIC ->
              io.github.zenhelix.gradle.plugin.client.model.PublishingType.AUTOMATIC
            PublishingType.USER_MANAGED ->
              io.github.zenhelix.gradle.plugin.client.model.PublishingType.USER_MANAGED
          }
        }
      )
      task.deploymentName.set(ext.deploymentName)

      task.maxRetriesStatusCheck.set(ext.uploader.maxRetriesStatusCheck)
      task.delayRetriesStatusCheck.set(ext.uploader.delayRetriesStatusCheck)

      task.zipFile.set(zipAggregateMavenCentralDeployment.flatMap { it.archiveFile })
    }
  }

// Configure the 'io.github.zenhelix.maven-central-publish' plugin to all projects
allprojects.forEach { p ->
  p.pluginManager.withPlugin("maven-publish") {
    p.pluginManager.apply("io.github.zenhelix.maven-central-publish")
    p.extensions.configure<MavenCentralUploaderExtension> {
      val aggregatedMavenCentralDeploymentZipPart by p.configurations.creating
      aggregatedMavenCentralDeploymentZipPart.description = "Maven central publication zip"
      val aggregatedMavenCentralDeploymentZipPartElements by p.configurations.creating
      aggregatedMavenCentralDeploymentZipPartElements.description =
        "Elements for the Maven central publication zip"
      aggregatedMavenCentralDeploymentZipPartElements.isCanBeResolved = false
      aggregatedMavenCentralDeploymentZipPartElements.extendsFrom(
        aggregatedMavenCentralDeploymentZipPart
      )
      aggregatedMavenCentralDeploymentZipPartElements.attributes {
        attribute(
          Usage.USAGE_ATTRIBUTE,
          project.getObjects().named(Usage::class.java, "publication"),
        )
      }

      val aggregatemavenCentralDeployment by
        p.tasks.registering {
          val zip = p.tasks.findByName("zipDeploymentMavenPublication") as ZipDeploymentTask
          dependsOn(zip)
          outputs.file(zip.archiveFile.get().asFile)
        }

      val artifact =
        p.artifacts.add(
          aggregatedMavenCentralDeploymentZipPart.name,
          aggregatemavenCentralDeployment,
        ) {
          builtBy(aggregatemavenCentralDeployment)
        }
      aggregatedMavenCentralDeploymentZipPart.outgoing.artifact(artifact)

      rootProject.dependencies.add(
        mavenCentralDeploymentZipAggregation.name,
        rootProject.dependencies.project(p.path, aggregatedMavenCentralDeploymentZipPart.name),
      )
    }
  }
}

val buildToolIntegrationGradle by
  tasks.registering(Exec::class) {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Gradle, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("${project.projectDir}/gradlew", "-p", workingDir, "test")
  }

val buildToolIntegrationMaven by
  tasks.registering(Exec::class) {
    group = "Verification"
    description =
      "Checks whether the bom works fine with Maven, requires preceding publishToMavenLocal in a separate Gradle invocation"

    workingDir = file("build-tools-integration-tests")
    commandLine("./mvnw", "--batch-mode", "clean", "test", "-Dnessie.version=${project.version}")
  }

val buildToolsIntegrationTest by
  tasks.registering {
    group = "Verification"
    description =
      "Checks whether the bom works fine with build tools, requires preceding publishToMavenLocal in a separate Gradle invocation"

    dependsOn(buildToolIntegrationGradle)
    dependsOn(buildToolIntegrationMaven)
  }

val buildToolsIntegrationClean by
  tasks.registering(Delete::class) {
    delete("build-tools-integration-tests/.gradle")
    delete("build-tools-integration-tests/build")
    delete("build-tools-integration-tests/target")
  }

val clean by tasks.getting(Delete::class) { dependsOn(buildToolsIntegrationClean) }

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
