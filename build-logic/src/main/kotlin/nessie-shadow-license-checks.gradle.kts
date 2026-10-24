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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
  id("nessie-shadow-jar")
  id("nessie-license-checks")
}

val shadowJar = tasks.named<ShadowJar>("shadowJar")

val generateLicenseInventory =
  tasks.register<GenerateShadowLicenseInventory>("generateLicenseInventory") {
    group = "verification"
    description = "Records actual selected Shadow dependency inputs with resolved Maven coordinates"
    selectedDependencies.from(shadowJar.map { it.includedDependencies })
    minimized.set(shadowJar.flatMap { it.minimizeJar })
    coordinatesByPath.set(
      shadowJar
        .flatMap { it.configurations }
        .map { configurations ->
          val artifacts = configurations.flatMap { it.resolvedConfiguration.resolvedArtifacts }
          artifacts
            .groupBy { it.file.absolutePath }
            .mapValues { (path, values) ->
              val coordinates = values.map { it.moduleVersion.id.toString() }.distinct()
              require(coordinates.size == 1) { "Ambiguous Shadow input coordinate: $path" }
              coordinates.single()
            }
        },
    )
    inventory.set(layout.buildDirectory.file("reports/licensee/shadow-inputs.json"))
  }

val verifyLicenseAttribution =
  tasks.register<VerifyNessieLicenseAttribution>("verifyLicenseAttribution") {
    group = "verification"
    description = "Verifies selected Shadow dependency policy coverage and packaged attribution"
    dependsOn("licensee")
    licenseeReport.set(layout.buildDirectory.file("reports/licensee/artifacts.json"))
    sbom.set(generateLicenseInventory.flatMap { it.inventory })
    licenseFile.set(layout.settingsDirectory.file("gradle/built-uber-dists/LICENSE-BINARY-DIST"))
    noticeFile.set(layout.settingsDirectory.file("gradle/built-uber-dists/NOTICE-BINARY-DIST"))
    projectGroups.set(setOf("org.projectnessie.nessie", "org.projectnessie.nessie-integrations"))
    generatedComponentNames.set(emptySet())
    deliveredAttributionArchives.from(shadowJar.flatMap { it.archiveFile })
    validationReport.set(layout.buildDirectory.file("reports/licensee/attribution-validation.txt"))
  }

// Transfer producer evidence for SBOMs as providers only, across the isolated script/classpath
// boundary to configureDistributionSbom() in
// gradle/license-check-plugin/src/main/kotlin/org/projectnessie/gradle/sbom/DistributionSbom.kt.

extra["sbomShadow"] =
  mapOf(
    "selected" to
      shadowJar.map {
        it.includedDependencies
      },
    "archive" to
      shadowJar.flatMap {
        it.archiveFile
      },
    "minimized" to
      shadowJar.flatMap {
        it.minimizeJar
      },
    "metadata" to
      shadowJar
        .flatMap { it.configurations }
        .map { configurations ->
          configurations
            .flatMap { it.resolvedConfiguration.resolvedArtifacts }
            .groupBy { it.file.absolutePath }
            .mapValues { (path, values) ->
              val metadata =
                values
                  .map { artifact ->
                    val id = artifact.moduleVersion.id
                    mapOf(
                      "group" to id.group,
                      "name" to id.name,
                      "version" to id.version,
                      "classifier" to artifact.classifier.orEmpty(),
                      "type" to artifact.type,
                    )
                  }
                  .distinct()
              require(metadata.size == 1) { "Ambiguous Shadow SBOM input: $path" }
              metadata.single()
            }
        },
  )
