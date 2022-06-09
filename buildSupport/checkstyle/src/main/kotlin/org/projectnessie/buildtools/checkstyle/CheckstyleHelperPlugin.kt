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

package org.projectnessie.buildtools.checkstyle

import io.quarkus.gradle.QuarkusPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.plugins.quality.CheckstyleExtension
import org.gradle.api.plugins.quality.CheckstylePlugin
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.withType

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class CheckstyleHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val hasSrcMain = projectDir.resolve("src/main").exists()
      val hasSrcTest = projectDir.resolve("src/test").exists()
      if (hasSrcMain || hasSrcTest) {
        tasks.withType<Test>().configureEach { dependsOn(tasks.named("processTestJandexIndex")) }

        apply<CheckstylePlugin>()
        configure<CheckstyleExtension> {
          toolVersion = dependencyVersion("versionCheckstyle")
          config = resources.text.fromFile(rootProject.file("codestyle/checkstyle-config.xml"))
          isShowViolations = true
          isIgnoreFailures = false
        }

        tasks.withType<Checkstyle>().configureEach {
          when (name) {
            "checkstyleMain" -> dependsOn(tasks.named("processJandexIndex"))
            "checkstyleTest" -> dependsOn(tasks.named("processTestJandexIndex"))
            else -> {}
          }
          if (plugins.findPlugin(QuarkusPlugin::class.java) != null) {
            when (name) {
              "checkstyleMain" -> dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava"))
              "checkstyleTest" -> dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
              else -> {}
            }
          }
          maxWarnings = 0 // treats warnings as errors
        }
      }
    }

  private fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()
}
