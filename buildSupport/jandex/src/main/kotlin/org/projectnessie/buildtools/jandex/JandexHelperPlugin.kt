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

package org.projectnessie.buildtools.jandex

import com.github.vlsi.jandex.JandexExtension
import com.github.vlsi.jandex.JandexPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.extra

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class JandexHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val hasSrcMain = projectDir.resolve("src/main").exists()
      val hasSrcTest = projectDir.resolve("src/test").exists()
      if (hasSrcMain || hasSrcTest) {
        apply<JandexPlugin>()
        configure<JandexExtension> { toolVersion.set(dependencyVersion("versionJandex")) }
      }
    }

  private fun Project.dependencyVersion(key: String) = rootProject.extra[key].toString()
}
