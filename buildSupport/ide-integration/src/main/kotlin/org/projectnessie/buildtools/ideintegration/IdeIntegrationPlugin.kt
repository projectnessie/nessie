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

package org.projectnessie.buildtools.ideintegration

import java.util.*
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.eclipse.model.EclipseModel
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.gradle.ext.*

/** Integrates into IntelliJ IDEA and Eclipse. */
@Suppress("unused")
class IdeIntegrationPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val ideName = "Nessie ${rootProject.version.toString().replace(Regex("^([0-9.]+).*"), "$1")}"

      apply<IdeaExtPlugin>()
      configure<IdeaModel> {
        module {
          name = ideName
          isDownloadSources = true // this is the default BTW
          inheritOutputDirs = true
        }

        this.project.settings {
          copyright {
            useDefault = "Nessie-ASF"
            profiles.create("Nessie-ASF") {
              // strip trailing LF
              val copyrightText =
                rootProject.file("codestyle/copyright-header.txt").readLines().joinToString("\n")
              notice = copyrightText
            }
          }

          encodings.encoding = "UTF-8"
          encodings.properties.encoding = "UTF-8"

          runConfigurations.register("Gradle", Gradle::class.java) {
            defaults = true

            jvmArgs =
              rootProject.projectDir
                .resolve("gradle.properties")
                .reader()
                .use {
                  val rules = Properties()
                  rules.load(it)
                  rules
                }
                .map { e -> "-D${e.key}=${e.value}" }
                .joinToString(" ")
          }

          delegateActions.testRunner = ActionDelegationConfig.TestRunner.CHOOSE_PER_TEST
        }
      }

      // There's no proper way to set the name of the IDEA project (when "just importing" or syncing
      // the
      // Gradle project)
      val ideaDir = projectDir.resolve(".idea")

      if (ideaDir.isDirectory) {
        ideaDir.resolve(".name").writeText(ideName)
      }

      apply<EclipsePlugin>()
      configure<EclipseModel> { project { name = ideName } }
    }
}
