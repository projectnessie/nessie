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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.api.plugins.quality.CheckstyleExtension
import org.gradle.api.plugins.quality.CheckstylePlugin
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.provideDelegate
import org.gradle.kotlin.dsl.withType

class NessieCheckstylePlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      if (project.name.endsWith("-proto")) {
        // Exclude projects that only generate Java from protobuf
        return
      }

      apply<CheckstylePlugin>()
      configure<CheckstyleExtension> {
        toolVersion = libsRequiredVersion("checkstyle")
        config = resources.text.fromFile(rootProject.file("codestyle/checkstyle-config.xml"))
        isShowViolations = true
        isIgnoreFailures = false
      }

      tasks.withType<Checkstyle>().configureEach {
        if (plugins.hasPlugin("io.quarkus")) {
          when (name) {
            "checkstyleMain" -> dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava"))
            "checkstyleTestFixtures" ->
              dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
            "checkstyleTest" -> dependsOn(tasks.named("compileQuarkusTestGeneratedSourcesJava"))
            else -> {}
          }
        }
        maxWarnings = 0 // treats warnings as errors
      }

      val sourceSets = project.extensions.findByType(SourceSetContainer::class.java)
      if (sourceSets != null) {
        val checkstyleAll =
          tasks.register("checkstyle") { description = "Checkstyle all source sets" }

        sourceSets.withType(SourceSet::class.java).configureEach {
          val sourceSet = this
          val checkstyleTask =
            tasks.named(sourceSet.getTaskName("checkstyle", null)) {
              dependsOn(sourceSet.getTaskName("process", "jandexIndex"))
            }
          checkstyleAll.configure { dependsOn(checkstyleTask) }
        }
      }
    }
}
