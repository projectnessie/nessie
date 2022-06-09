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

package org.projectnessie.buildtools.smallryeopenapi

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.internal.jvm.ClassDirectoryBinaryNamingScheme
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.SourceSet
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.named
import org.gradle.language.jvm.tasks.ProcessResources

@Suppress("unused")
class SmallryeOpenApiPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      val ext = extensions.create<SmallryeOpenApiExtension>("smallryeOpenApi", this)

      val javaExtension = project.extensions.getByType(JavaPluginExtension::class.java)
      val sourceSet = javaExtension.sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
      val namingScheme = ClassDirectoryBinaryNamingScheme(sourceSet.name)
      val genTaskName = namingScheme.getTaskName(null, "generateOpenApiSpec")

      val configProvider = project.configurations.named(sourceSet.compileClasspathConfigurationName)

      val resourcesSrcDirs = project.objects.fileCollection()
      resourcesSrcDirs.from(sourceSet.resources.srcDirs)

      tasks
        .register(
          genTaskName,
          SmallryeOpenApiTask::class.java,
          ext,
          configProvider,
          resourcesSrcDirs,
          sourceSet.output.classesDirs
        )
        .configure {
          group = "Smallrye OpenAPI"
          description = "Generate OpenAPI spec"
          dependsOn(sourceSet.compileJavaTaskName)
          inputs.files(sourceSet.output.dirs).withPathSensitivity(PathSensitivity.RELATIVE)
          inputs.files(configProvider).withPathSensitivity(PathSensitivity.RELATIVE)
        }

      tasks.named<ProcessResources>(sourceSet.processResourcesTaskName).configure {
        dependsOn(genTaskName)
        from(tasks.getByName(genTaskName).outputs.files)
      }
    }
}
