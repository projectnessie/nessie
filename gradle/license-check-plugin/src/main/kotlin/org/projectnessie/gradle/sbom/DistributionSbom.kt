/*
 * Copyright (C) 2026 Dremio
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

package org.projectnessie.gradle.sbom

import org.cyclonedx.gradle.CyclonedxDirectTask
import org.cyclonedx.gradle.CyclonedxPlugin
import org.gradle.api.Project
import org.gradle.api.file.ConfigurableFileCollection
import org.gradle.api.file.RegularFile
import org.gradle.api.plugins.ExtraPropertiesExtension
import org.gradle.api.provider.Provider
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import org.projectnessie.gradle.sbom.PackageKind.QUARKUS_FAST_JAR
import org.projectnessie.gradle.sbom.PackageKind.QUARKUS_UBER_JAR
import org.projectnessie.gradle.sbom.PackageKind.SHADED_JAR

/** Nessie producer and publication wiring; keep separate. */
@Suppress("unused")
fun Project.configureDistributionSbom() {
  // The producers own SBOM generation. One finalizer supplies the final artifact
  // identity/selection.
  val subject = subjectForGradleProject(project)
  if (subject.packageKind == SHADED_JAR) {
    pluginManager.apply(CyclonedxPlugin::class.java)
    tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom") {
      includeConfigs.set(listOf("runtimeClasspath"))
      includeBomSerialNumber.set(false)
      includeBuildSystem.set(false)
      xmlOutput.unsetConvention()
    }
  }

  val shadowInputs = extra

  val finalSbom =
    tasks.register<FinalizeDistributionSbom>("finalizeDistributionSbom") {
      group = "build"
      description = "Adds final subject identity and actual assembly inputs to the producer SBOM"
      this.subject.set(subject)
      when (subject.packageKind) {
        SHADED_JAR -> {
          val sbomShadow = SbomShadow.sbomShadowFromExtra(shadowInputs)
          subjectFile.set(sbomShadow.archive)
          selectedFiles.from(sbomShadow.selected)
          metadataByPath.set(sbomShadow.metadata)
          minimized.set(sbomShadow.minimized)
          producer.set(
            tasks.named<CyclonedxDirectTask>("cyclonedxDirectBom").flatMap { it.jsonOutput },
          )
        }
        QUARKUS_FAST_JAR -> {
          dependsOn("quarkusBuild")
          subjectTree.set(layout.buildDirectory.dir("quarkus-app"))
          treeDescriptor.set(layout.buildDirectory.file("reports/sbom/subject.json"))
          producer.set(layout.buildDirectory.file("quarkus-run-cyclonedx.json"))
        }
        QUARKUS_UBER_JAR -> {
          dependsOn("quarkusBuild")
          subjectFile.set(layout.buildDirectory.file("${subject.nameAndVersion}-runner.jar"))
          producer.set(
            layout.buildDirectory.file("${subject.nameAndVersion}-runner-cyclonedx.json"),
          )
        }
      }
      document.set(
        layout.buildDirectory.file(
          "reports/sbom/${subject.nameAndVersion}${subject.packageKind.sbomBasenameSuffix}",
        ),
      )
    }

  val distributionLicenseCheck = tasks.named("checkDistributionLicenses")

  afterEvaluate {
    // Realize publications outside lazy publication actions, and retain the existing license gate.
    extensions.getByType<PublishingExtension>().publications.withType<MavenPublication>().forEach {
      publication ->
      if (subject.packageKind.uberLike) {
        publication.artifact(finalSbom.flatMap { it.document }) {
          classifier = subject.packageKind.sbomClassifier
          extension = "json"
        }
      }
      publication.artifacts.configureEach { builtBy(distributionLicenseCheck) }
    }
  }
}

private fun subjectForGradleProject(project: Project): SbomSubject =
  SbomSubject(
    project.group.toString(),
    project.name,
    project.version.toString(),
    if (project.plugins.hasPlugin("nessie-shadow-license-checks")) {
      SHADED_JAR
    } else if (project.providers.gradleProperty("uber-jar").isPresent) {
      QUARKUS_UBER_JAR
    } else {
      QUARKUS_FAST_JAR
    },
  )

private class SbomShadow(
  val archive: Provider<RegularFile>,
  val selected: Provider<ConfigurableFileCollection>,
  val metadata: Provider<Map<String, Map<String, String>>>,
  val minimized: Provider<Boolean>,
) {
  companion object {
    @Suppress("UNCHECKED_CAST")
    fun sbomShadowFromExtra(extra: ExtraPropertiesExtension): SbomShadow {
      // Produced in build-logic/src/main/kotlin/nessie-shadow-license-checks.gradle.kts
      val sbomShadow = extra["sbomShadow"] as Map<String, Any>
      return SbomShadow(
        sbomShadow["archive"] as Provider<RegularFile>,
        sbomShadow["selected"] as Provider<ConfigurableFileCollection>,
        sbomShadow["metadata"] as Provider<Map<String, Map<String, String>>>,
        sbomShadow["minimized"] as Provider<Boolean>,
      )
    }
  }
}
