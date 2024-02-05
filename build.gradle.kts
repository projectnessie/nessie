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

import org.jetbrains.changelog.date
import org.jetbrains.gradle.ext.settings
import org.jetbrains.gradle.ext.taskTriggers

plugins {
  eclipse
  id("nessie-conventions-root")
  alias(libs.plugins.nexus.publish.plugin)
  alias(libs.plugins.jetbrains.changelog)
}

apply<ReleaseSupportPlugin>()

extra["maven.name"] = "Nessie"

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
    "versionJacoco" to libs.versions.jacoco.get(),
    "versionJandex" to libs.versions.jandex.get()
  )
  .plus(loadProperties(file("integrations/spark-scala.properties")))
  .forEach { (k, v) -> extra[k.toString()] = v }

tasks.named<Wrapper>("wrapper").configure { distributionType = Wrapper.DistributionType.ALL }

// Pass environment variables:
//    ORG_GRADLE_PROJECT_sonatypeUsername
//    ORG_GRADLE_PROJECT_sonatypePassword
// OR in ~/.gradle/gradle.properties set
//    sonatypeUsername
//    sonatypePassword
// Call targets:
//    publishToSonatype
//    closeAndReleaseSonatypeStagingRepository
nexusPublishing {
  transitionCheckOptions {
    // default==60 (10 minutes), wait up to 60 minutes
    maxRetries = 360
    // default 10s
    delayBetween = java.time.Duration.ofSeconds(10)
  }
  repositories { sonatype() }
}

val buildToolIntegrationGradle by tasks.registering(Exec::class)

buildToolIntegrationGradle.configure {
  group = "Verification"
  description =
    "Checks whether the bom works fine with Gradle, requires preceding publishToMavenLocal in a separate Gradle invocation"

  workingDir = file("build-tools-integration-tests")
  commandLine("${project.projectDir}/gradlew", "-p", workingDir, "test")
}

val buildToolIntegrationMaven by tasks.registering(Exec::class)

buildToolIntegrationMaven.configure {
  group = "Verification"
  description =
    "Checks whether the bom works fine with Maven, requires preceding publishToMavenLocal in a separate Gradle invocation"

  workingDir = file("build-tools-integration-tests")
  commandLine("./mvnw", "--batch-mode", "clean", "test", "-Dnessie.version=${project.version}")
}

val buildToolsIntegrationTest by tasks.registering

buildToolsIntegrationTest.configure {
  group = "Verification"
  description =
    "Checks whether the bom works fine with build tools, requires preceding publishToMavenLocal in a separate Gradle invocation"

  dependsOn(buildToolIntegrationGradle)
  dependsOn(buildToolIntegrationMaven)
}

publishingHelper {
  nessieRepoName = "nessie"
  inceptionYear = "2020"
}

spotless {
  kotlinGradle {
    // Must be repeated :( - there's no "addTarget" or so
    target("nessie-iceberg/*.gradle.kts", "*.gradle.kts", "build-logic/*.gradle.kts")
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
      "Commits"
    )
  version = provider { project.version.toString() }
}

idea.project.settings {
  taskTriggers {
    afterSync(
      ":nessie-protobuf-relocated:jar",
      ":nessie-spark-antlr-runtime:jar",
      ":nessie-spark-extensions-grammar:jar"
    )
  }
}
