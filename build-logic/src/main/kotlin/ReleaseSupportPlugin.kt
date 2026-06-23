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

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.options.Option
import org.gradle.kotlin.dsl.register
import org.gradle.work.DisableCachingByDefault

/** Registers some tasks to manage the `version.txt` file. */
class ReleaseSupportPlugin : Plugin<Project> {
  override fun apply(project: Project) {

    project.extensions.create("releaseSupport", ReleaseSupport::class.java)

    project.tasks.register<ShowVersionTask>("showVersion") {
      group = "Release Support"
      description = "Show current version"
      versionFile.set(project.extensions.getByType(ReleaseSupport::class.java).versionFile)
    }

    project.tasks.register<BumpVersionTask>("bumpVersion") {
      group = "Release Support"
      description =
        "Bumps the version to the next patch/minor/major version as a snapshot, see ' ./gradlew help --task :bumpVersion '."
      versionFile.set(project.extensions.getByType(ReleaseSupport::class.java).versionFile)
    }
  }

  open class ReleaseSupport(project: Project) {
    val versionFile: RegularFileProperty =
      project.objects
        .fileProperty()
        .fileProvider(project.provider { project.rootDir.resolve("./version.txt") })
  }

  @DisableCachingByDefault(because = "Version information cannot be cached")
  abstract class ShowVersionTask : DefaultTask() {
    @get:InputFile abstract val versionFile: RegularFileProperty

    @TaskAction
    fun showVersion() {
      logger.lifecycle(
        "Current version is ${VersionTuple.fromFile(versionFile.get().asFile.toPath())}."
      )
    }
  }

  @DisableCachingByDefault(because = "Version bumps cannot be cached")
  abstract class BumpVersionTask : DefaultTask() {
    @get:InputFile @get:OutputFile abstract val versionFile: RegularFileProperty

    @Option(
      option = "bumpToRelease",
      description = "Define whether to bump to a release version, defaults to snapshot release.",
    )
    @Internal
    var bumpToRelease: Boolean = false

    @Option(
      option = "bumpType",
      description = "Defines which part of the version should be bumped, defaults to 'none'.",
    )
    @Internal
    var bumpType: BumpType = BumpType.none

    @TaskAction
    fun bumpVersion() {
      val versionFilePath = versionFile.get().asFile.toPath()
      val currentVersion = VersionTuple.fromFile(versionFilePath)

      logger.lifecycle("Current version is $currentVersion.")

      val nextVersion =
        when (bumpType) {
          BumpType.none -> currentVersion
          BumpType.patch -> currentVersion.bumpPatch()
          BumpType.minor -> currentVersion.bumpMinor()
          BumpType.major -> currentVersion.bumpMajor()
        }

      val finalVersion = if (bumpToRelease) nextVersion.asRelease() else nextVersion.asSnapshot()

      if (finalVersion < currentVersion) {
        throw GradleException(
          "New version $finalVersion would be lower than current version $currentVersion"
        )
      }

      if (finalVersion != currentVersion) {
        finalVersion.writeToFile(versionFilePath)
        logger.lifecycle("New version is $finalVersion.")
      } else {
        throw GradleException("Bump version tasks results in no change.")
      }
    }
  }

  @Suppress("EnumEntryName")
  enum class BumpType {
    // lower-case, used as command line option values
    none,
    patch,
    minor,
    major,
  }
}
