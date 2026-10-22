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

package org.projectnessie.gradle.licenses

import app.cash.licensee.LicenseeExtension
import app.cash.licensee.LicenseePlugin
import app.cash.licensee.LicenseeTask
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named

class LicenseChecksPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    project.pluginManager.apply(LicenseePlugin::class.java)
    val licensee = project.extensions.getByType<LicenseeExtension>()
    project.extensions.create(
      "licenseChecks",
      LicenseChecksExtension::class.java,
      project.providers,
      licensee,
    )
    licensee.ignoreDependencies("org.projectnessie.nessie") { because("Nessie project artifacts") }
    licensee.ignoreDependencies("org.projectnessie.nessie-integrations") {
      because("Nessie integration artifacts")
    }
    project.tasks.named<LicenseeTask>("licensee") {
      if (project.plugins.hasPlugin("nessie-quarkus-license-checks")) {
        configurationToCheck(
          project.configurations.named("quarkusProdRuntimeClasspathConfiguration"),
        )
        dependsOn("quarkusAppPartsBuild")
      } else {
        configurationToCheck(project.configurations.named("runtimeClasspath"))
      }
    }
  }
}
