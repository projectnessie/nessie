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

import io.quarkus.gradle.QuarkusPlugin
import org.gradle.api.NamedDomainObjectProvider
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.attributes.Category
import org.gradle.api.attributes.DocsType
import org.gradle.api.attributes.Usage
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.named
import org.gradle.kotlin.dsl.withType
import org.gradle.testing.jacoco.plugins.JacocoPlugin
import org.gradle.testing.jacoco.plugins.JacocoPluginExtension
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension
import org.gradle.testing.jacoco.tasks.JacocoReport

/** Applies common configurations to all Nessie projects. */
@Suppress("unused")
class JacocoHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      if (!java.lang.Boolean.getBoolean("idea.sync.active")) {
        if (this != rootProject) {
          plugins.withType<JacocoPlugin>().configureEach {
            if (path != ":code-coverage") {
              val coverageDataElements = addJacocoConfigurations()

              // Causes the test tasks to run if the coverage data is requested by the aggregation
              // task
              tasks.withType<Test>().configureEach {
                val test = this
                val jacocoExtension = extensions.findByType(JacocoTaskExtension::class.java)
                if (jacocoExtension != null) {
                  coverageDataElements.configure {
                    // Note: do *not* relativize the destinationFile below, otherwise overall
                    // code-coverage breaks.
                    outgoing.artifact(jacocoExtension.destinationFile!!) { builtBy(test) }
                  }
                }
              }
            }

            configure<JacocoPluginExtension> { toolVersion = dependencyVersion("versionJacoco") }

            tasks.withType<JacocoReport>().configureEach {
              reports {
                html.required.set(true)
                xml.required.set(true)
              }
            }
          }

          plugins.withType<QuarkusPlugin>().configureEach {
            val coverageDataElements = addJacocoConfigurations()
            coverageDataElements.configure {
              val jacocoFile = buildDir.resolve("jacoco-quarkus.exec")
              outgoing.artifact(jacocoFile) { builtBy(tasks.named("test")) }
            }
          }
        }
      }
    }

  private fun Project.addJacocoConfigurations(): NamedDomainObjectProvider<Configuration> {
    // Share sources folder with other projects for aggregated JaCoCo reports
    configurations.register("transitiveSourcesElements") {
      isVisible = false
      isCanBeResolved = false
      isCanBeConsumed = true
      attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
        attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("source-folders"))
      }
      val sourceSets = project.extensions.getByType<JavaPluginExtension>().sourceSets
      val mainSourceSet = sourceSets.named("main").get()
      mainSourceSet.java.srcDirs.forEach {
        outgoing.artifact(it) { builtBy(tasks.named(mainSourceSet.classesTaskName)) }
      }
    }

    // Share classes folder with other projects for aggregated JaCoCo reports
    configurations.register("transitiveClassesElements") {
      isVisible = false
      isCanBeResolved = false
      isCanBeConsumed = true
      attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
        attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("class-folders"))
      }
      val sourceSets = project.extensions.getByType<JavaPluginExtension>().sourceSets
      val mainSourceSet = sourceSets.named("main").get()
      outgoing.artifact(mainSourceSet.java.destinationDirectory) {
        builtBy(tasks.named(mainSourceSet.classesTaskName))
      }
    }

    // Share the coverage data to be aggregated for the whole product
    return configurations.register("coverageDataElements") {
      isVisible = false
      isCanBeResolved = false
      isCanBeConsumed = true
      attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.DOCUMENTATION))
        attribute(DocsType.DOCS_TYPE_ATTRIBUTE, objects.named("jacoco-coverage-data"))
      }
    }
  }
}
