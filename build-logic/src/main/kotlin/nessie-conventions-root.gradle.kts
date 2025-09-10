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

// Nessie root project

import copiedcode.CopiedCodeCheckerPlugin
import java.util.Properties
import org.jetbrains.gradle.ext.ActionDelegationConfig
import org.jetbrains.gradle.ext.copyright
import org.jetbrains.gradle.ext.delegateActions
import org.jetbrains.gradle.ext.encodings
import org.jetbrains.gradle.ext.runConfigurations
import org.jetbrains.gradle.ext.settings

plugins {
  `maven-publish`
  signing
  eclipse
  id("org.jetbrains.gradle.plugin.idea-ext")
  id("nessie-common-base")
}

loadNessieProjects(rootProject)

val projectName = rootProject.file("ide-name.txt").readText().trim()
val ideName =
  "$projectName ${rootProject.version.toString().replace(Regex("^([0-9.]+).*"), "$1")} [in ../${rootProject.rootDir.name}]"

if (System.getProperty("idea.sync.active").toBoolean()) {

  idea {
    module {
      name = ideName
      isDownloadSources = true // this is the default BTW
      inheritOutputDirs = true

      // The NesQuEIT project includes the Nessie sources as two Gradle builds - one for everything
      // except Iceberg and one for the rest that has dependencies to Iceberg, which uses
      // `nessie-iceberg/` as the build root directory. This variable needs to refer to the Nessie
      // "main source" root directory.
      val nessieRootProjectDir =
        if (projectDir.resolve("integrations").exists()) projectDir else projectDir.resolve("..")
      val integrationsDir = nessieRootProjectDir.resolve("integrations")
      val sparkExtensionsDir = integrationsDir.resolve("spark-extensions")
      val buildToolsIT = nessieRootProjectDir.resolve("build-tools-integration-tests")

      val sparkScalaProps = Properties()
      integrationsDir.resolve("spark-scala.properties").reader().use { sparkScalaProps.load(it) }

      excludeDirs =
        excludeDirs +
          setOf(
            // Do not index the .mvn folders
            nessieRootProjectDir.resolve(".mvn"),
            // And more...
            nessieRootProjectDir.resolve(".idea"),
            nessieRootProjectDir.resolve("site/venv"),
            nessieRootProjectDir.resolve("nessie-iceberg/.gradle"),
            buildToolsIT.resolve(".gradle"),
            buildToolsIT.resolve("build"),
            buildToolsIT.resolve("target"),
            // somehow those are not automatically excluded...
            integrationsDir.resolve("spark-extensions-base/build"),
            integrationsDir.resolve("spark-extensions-basetests/build"),
          ) +
          allprojects.map { prj -> prj.layout.buildDirectory.asFile.get() } +
          sparkScalaProps
            .getProperty("sparkVersions")
            .split(",")
            .map { sparkVersion -> sparkExtensionsDir.resolve("v$sparkVersion/build") }
            .toSet()
    }

    this.project.settings {
      copyright {
        useDefault = "Nessie-ASF"
        profiles.create("Nessie-ASF") {
          // strip trailing LF
          val copyrightText =
            rootProject.file("codestyle/copyright-header.txt").readLines().joinToString("\n")
          notice = copyrightText
        }
      }

      encodings.encoding = "UTF-8"
      encodings.properties.encoding = "UTF-8"

      val runConfig =
        runConfigurations.register("Gradle", org.jetbrains.gradle.ext.Gradle::class.java)
      runConfig.configure {
        defaults = true

        jvmArgs =
          rootProject.projectDir
            .resolve("gradle.properties")
            .reader()
            .use {
              val rules = Properties()
              rules.load(it)
              rules
            }
            .map { e -> "-D${e.key}=${e.value}" }
            .joinToString(" ")
      }

      delegateActions.testRunner =
        ActionDelegationConfig.TestRunner.valueOf(
          System.getProperty("nessie.intellij.test-runner", "CHOOSE_PER_TEST")
        )
    }
  }

  // There's no proper way to set the name of the IDEA project (when "just importing" or
  // syncing
  // the Gradle project)
  val ideaDir = projectDir.resolve(".idea")

  if (ideaDir.isDirectory) {
    ideaDir.resolve(".name").writeText(ideName)
  }
}

eclipse { project { name = ideName } }

tasks.register("listProjectsWithPrefix", ListChildProjectsTask::class)

@DisableCachingByDefault(because = "Projects list is not worth caching")
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

apply<CopiedCodeCheckerPlugin>()

allprojects {
  tasks.register("codeChecks").configure {
    group = "build"
    description = "Runs code style and license checks"
  }
}
