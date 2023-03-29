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

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.withType
import org.gradle.testing.jacoco.plugins.JacocoPlugin
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.plugins.JacocoReportAggregationPlugin
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension
import org.gradle.testing.jacoco.tasks.JacocoReport

class NessieCodeCoveragePlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val path = project.path
      if (
        path == ":iceberg-views" ||
          (path.startsWith(":nessie-") &&
            path != ":nessie-bom" &&
            path != ":nessie-compatibility-tests")
      ) {
        apply<JacocoPlugin>()
        @Suppress("UnstableApiUsage") apply<JacocoReportAggregationPlugin>()

        tasks.withType<JacocoReport>().configureEach {
          reports {
            html.required.set(true)
            xml.required.set(true)
          }
        }

        configure<JacocoPluginExtension> { toolVersion = libsRequiredVersion("jacoco") }

        if (plugins.hasPlugin("io.quarkus")) {
          tasks.named("classes") { dependsOn(tasks.named("compileQuarkusGeneratedSourcesJava")) }

          tasks.withType<Test>().configureEach {
            extensions.configure(JacocoTaskExtension::class.java) {
              val excluded = excludeClassLoaders
              excludeClassLoaders =
                listOf("*QuarkusClassLoader") + (if (excluded != null) excluded else emptyList())
            }
            systemProperty("quarkus.jacoco.report", "false")
            systemProperty("quarkus.jacoco.reuse-data-file", "true")
          }
        }
      }
    }
}
