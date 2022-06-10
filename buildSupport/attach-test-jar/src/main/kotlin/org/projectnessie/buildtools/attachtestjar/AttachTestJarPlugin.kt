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

package org.projectnessie.buildtools.attachtestjar

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.kotlin.dsl.configure

/** Attaches a testJar artifact to the project's output. */
@Suppress("unused")
class AttachTestJarPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      configure<JavaPluginExtension> {
        registerFeature("tests") {
          usingSourceSet(sourceSets.getByName("test"))
          withJavadocJar()
          withSourcesJar()
        }
      }
    }
}
