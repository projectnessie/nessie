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

package org.projectnessie.buildtools.protobuf

import com.google.protobuf.gradle.ProtobufPlugin
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.quality.Checkstyle
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.withType

/** Makes the generated sources available to IDEs, disables Checkstyle on generated code. */
@Suppress("unused")
class ProtobufHelperPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      apply<ProtobufPlugin>()
      plugins.withType<ProtobufPlugin>().configureEach {
        val sourceSets = project.extensions.getByType<JavaPluginExtension>().sourceSets

        val sourceSetJavaMain = sourceSets.getByName("main").java
        sourceSetJavaMain.srcDir(project.buildDir.resolve("generated/source/proto/main/java"))
        sourceSetJavaMain.destinationDirectory.set(
          project.buildDir.resolve("classes/java/generated")
        )

        val sourceSetJavaTest = sourceSets.getByName("test").java
        sourceSetJavaTest.srcDir(project.buildDir.resolve("generated/source/proto/test/java"))
        sourceSetJavaTest.destinationDirectory.set(
          project.buildDir.resolve("classes/java/generatedTest")
        )
      }

      tasks.withType(Checkstyle::class.java).configureEach {
        exclude("org/projectnessie/**/*Types.java")
      }
    }
}
