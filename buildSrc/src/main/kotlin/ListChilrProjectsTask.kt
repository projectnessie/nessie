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

import org.gradle.api.DefaultTask
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.options.Option
import org.gradle.work.DisableCachingByDefault

@DisableCachingByDefault(because = "Projects list is worth caching")
abstract class ListChildProjectsTask : DefaultTask() {
  @get:Option(option = "prefix", description = "Project path prefix.")
  @get:Internal
  abstract val prefix: Property<String>

  @get:Option(option = "task", description = "Name of the task to print.")
  @get:Internal
  abstract val task: Property<String>

  @get:Option(option = "output", description = "Output file name.")
  @get:Internal
  abstract val output: Property<String>

  @get:Option(option = "exclude", description = "Output file name.")
  @get:Internal
  abstract val exclude: Property<Boolean>

  @TaskAction
  fun exec() {
    val prefix = prefix.get()
    val task = task.convention("intTest").get()
    val outputFile = output.get()
    val exclude = if (exclude.convention(false).get()) "-x " else ""
    project.file(outputFile).writer().use {
      val writer = it
      project.childProjects.values
        .filter { it.path.startsWith(prefix) }
        .forEach { writer.write("$exclude${it.path}:$task\n") }
    }
  }
}
