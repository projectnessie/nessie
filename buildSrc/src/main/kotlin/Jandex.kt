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

import com.github.vlsi.jandex.JandexBuildAction
import com.github.vlsi.jandex.JandexExtension
import com.github.vlsi.jandex.JandexPlugin
import com.github.vlsi.jandex.JandexProcessResources
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.apply
import org.gradle.kotlin.dsl.configure
import org.gradle.kotlin.dsl.provideDelegate

class NessieJandexPlugin : Plugin<Project> {
  override fun apply(project: Project): Unit =
    project.run {
      apply<JandexPlugin>()
      configure<JandexExtension> { toolVersion.set(libsRequiredVersion("jandex")) }

      val sourceSets: SourceSetContainer? by project
      sourceSets?.withType(SourceSet::class.java)?.configureEach {
        val sourceSet = this

        val jandexTaskName = sourceSet.getTaskName("process", "jandexIndex")
        tasks.named(jandexTaskName, JandexProcessResources::class.java) {
          if ("main" != sourceSet.name) {
            // No Jandex for non-main
            jandexBuildAction.set(JandexBuildAction.NONE)
          }
          if (!project.plugins.hasPlugin("io.quarkus")) {
            dependsOn(tasks.named(sourceSet.classesTaskName))
          }
        }
      }
    }
}
