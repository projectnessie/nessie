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

package org.projectnessie.buildtools.errorprone

import java.util.Properties
import net.ltgt.gradle.errorprone.CheckSeverity
import net.ltgt.gradle.errorprone.ErrorPronePlugin
import net.ltgt.gradle.errorprone.errorprone
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.extra
import org.gradle.kotlin.dsl.withType

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class ErrorproneHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      apply<ErrorPronePlugin>()
      tasks.withType<JavaCompile>().configureEach {
        options.errorprone.allErrorsAsWarnings.set(true)
        options.errorprone.disableWarningsInGeneratedCode.set(true)

        val errorproneRules =
          rootProject.projectDir.resolve("codestyle/errorprone-rules.properties")
        inputs.file(errorproneRules).withPathSensitivity(PathSensitivity.RELATIVE)

        val checksMapProperty =
          objects
            .mapProperty(String::class.java, CheckSeverity::class.java)
            .convention(
              provider {
                val checksMap = HashMap<String, CheckSeverity>()
                errorproneRules.reader().use {
                  val rules = Properties()
                  rules.load(it)
                  rules.forEach { k, v ->
                    val key = k as String
                    val value = v as String
                    if (key.isNotEmpty() && value.isNotEmpty()) {
                      checksMap[key.trim()] = CheckSeverity.valueOf(value.trim())
                    }
                  }
                }
                checksMap
              }
            )

        options.errorprone.checks.putAll(checksMapProperty)
        options.errorprone.excludedPaths.set(".*/build/generated.*")
      }
      plugins.withType<JavaPlugin>().configureEach {
        configure<JavaPluginExtension> {
          sourceSets.configureEach {
            dependencies {
              add(
                "errorprone",
                "com.google.errorprone:error_prone_core:${dependencyVersion("versionErrorProneCore")}"
              )
              add(
                "errorprone",
                "jp.skypencil.errorprone.slf4j:errorprone-slf4j:${dependencyVersion("versionErrorProneSlf4j")}"
              )
            }
          }
        }
      }
    }

  private fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()
}
