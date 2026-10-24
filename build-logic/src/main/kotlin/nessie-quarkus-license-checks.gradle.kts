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

plugins { id("nessie-license-checks") }

val verifyLicenseAttribution =
  tasks.register<VerifyNessieLicenseAttribution>("verifyLicenseAttribution") {
    group = "verification"
    description = "Verifies shipped dependency policy coverage and delivered LICENSE/NOTICE"
    dependsOn("licensee", "quarkusBuild")
    licenseeReport.set(layout.buildDirectory.file("reports/licensee/artifacts.json"))
    val uberJar = providers.gradleProperty("uber-jar").isPresent
    val runnerName = if (uberJar) "${project.name}-${project.version}-runner" else "quarkus-run"
    sbom.set(layout.buildDirectory.file("$runnerName-cyclonedx.json"))
    licenseFile.set(layout.settingsDirectory.file("gradle/built-uber-dists/LICENSE-BINARY-DIST"))
    noticeFile.set(layout.settingsDirectory.file("gradle/built-uber-dists/NOTICE-BINARY-DIST"))
    projectGroups.set(setOf("org.projectnessie.nessie", "org.projectnessie.nessie-integrations"))
    // Producer-owned generated files, not third-party dependencies with inferred provenance.
    generatedComponentNames.set(
      setOf(
        "generated-bytecode.jar",
        "quarkus-app-dependencies.txt",
        "quarkus-application.dat",
        "quarkus-run.jar",
        "transformed-bytecode.jar",
      ),
    )
    val archive =
      if (uberJar) {
        layout.buildDirectory.file("${project.name}-${project.version}-runner.jar")
      } else {
        layout.buildDirectory.file(
          "quarkus-app/lib/main/org.projectnessie.nessie.nessie-notice-${project.version}.jar",
        )
      }
    deliveredAttributionArchives.from(archive)
    validationReport.set(layout.buildDirectory.file("reports/licensee/attribution-validation.txt"))
  }
