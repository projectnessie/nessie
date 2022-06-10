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

package org.projectnessie.buildtools.jacoco

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.DocsType
import org.gradle.api.attributes.Usage
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.hasPlugin
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.project
import org.gradle.kotlin.dsl.withType
import org.gradle.testing.jacoco.tasks.JacocoReport

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class JacocoAggregatorHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {

      // Mechanism for multi-project jacoco report taken from
      // https://docs.gradle.org/current/samples/sample_jvm_multi_project_with_code_coverage.html

      // A resolvable configuration to collect source code
      val sourcesPath =
        configurations.create("sourcesPath") {
          isVisible = false
          isCanBeResolved = true
          isCanBeConsumed = false
          attributes {
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
            attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("source-folders"))
          }
        }

      // A resolvable configuration to collect classes folders
      val classesPath =
        configurations.create("classesPath") {
          isVisible = false
          isCanBeResolved = true
          isCanBeConsumed = false
          attributes {
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
            attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("class-folders"))
          }
        }

      // A resolvable configuration to collect JaCoCo coverage data
      val coverageDataPath =
        configurations.create("coverageDataPath") {
          isVisible = false
          isCanBeResolved = true
          isCanBeConsumed = false
          attributes {
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
            attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("jacoco-coverage-data"))
          }
        }

      // Task to gather code coverage from multiple subprojects
      val codeCoverageReport =
        tasks.register("codeCoverageReport", JacocoReport::class.java) {
          group = "Verification"
          description = "Generate code-coverage-report for all projects"
          additionalClassDirs(classesPath.incoming.artifactView { lenient(true) }.files)
          additionalSourceDirs(sourcesPath.incoming.artifactView { lenient(true) }.files)
          executionData(
            coverageDataPath.incoming.artifactView { lenient(true) }.files.filter { it.exists() }
          )
        }

      dependencies {
        val deps = this
        rootProject.allprojects {
          val prj = this
          tasks.withType<Test> {
            if (plugins.hasPlugin(JavaLibraryPlugin::class)) {
              deps.add(coverageDataPath.name, deps.project(prj.path, "coverageDataElements"))
              deps.add(sourcesPath.name, deps.project(prj.path, "transitiveSourcesElements"))
              deps.add(classesPath.name, deps.project(prj.path, "transitiveClassesElements"))
            }
          }
        }
      }

      tasks.named("check") { dependsOn(codeCoverageReport) }
    }
}
