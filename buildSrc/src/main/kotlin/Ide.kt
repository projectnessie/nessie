/*
 * Copyright (C) 2023 Dremio
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

import org.gradle.api.Project
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.plugins.ide.eclipse.EclipsePlugin
import org.gradle.plugins.ide.eclipse.model.EclipseModel
import org.gradle.plugins.ide.idea.model.IdeaModel
import org.jetbrains.gradle.ext.ActionDelegationConfig
import org.jetbrains.gradle.ext.IdeaExtPlugin
import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.delegateActions
import org.jetbrains.gradle.ext.encodings
import org.jetbrains.gradle.ext.runConfigurations
import org.jetbrains.gradle.ext.settings

fun Project.nessieIde() {
  apply<EclipsePlugin>()

  if (this == rootProject) {

    val projectName = rootProject.file("ide-name.txt").readText().trim()
    val ideName =
      "$projectName ${rootProject.version.toString().replace(Regex("^([0-9.]+).*"), "$1")}"

    apply<IdeaExtPlugin>()
    configure<IdeaModel> {
      module {
        name = ideName
        isDownloadSources = true // this is the default BTW
        inheritOutputDirs = true
      }

      project.settings {
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

        runConfigurations.register("Gradle", org.jetbrains.gradle.ext.Gradle::class.java) {
          defaults = true

          jvmArgs =
            rootProject.projectDir
              .resolve("gradle.properties")
              .reader()
              .use {
                val rules = java.util.Properties()
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
    // the Gradle project)
    val ideaDir = projectDir.resolve(".idea")

    if (ideaDir.isDirectory) {
      ideaDir.resolve(".name").writeText(ideName)
    }

    configure<EclipseModel> { project { name = ideName } }
  }
}
