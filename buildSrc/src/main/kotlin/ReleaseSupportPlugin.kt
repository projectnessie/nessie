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

import java.nio.file.Files
import java.nio.file.Path
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.Internal
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.options.Option
import org.gradle.kotlin.dsl.register
import org.gradle.work.DisableCachingByDefault

/** Registers some tasks to manage the `version.txt` file. */
class ReleaseSupportPlugin : Plugin<Project> {
  override fun apply(project: Project) {

    project.tasks.register("showVersion") {
      group = "Release Support"
      description = "Show current version"

      doFirst {
        logger.lifecycle(
          "Current version is ${VersionTuple.fromFile(versionTxtFile(this.project))}."
        )
      }
    }

    project.tasks.register<BumpVersionTask>("bumpVersion") {
      group = "Release Support"
      description =
        "Bumps the version to the next patch/minor/major version as a snapshot, see ' ./gradlew help --task :bumpVersion '."
    }
  }

  companion object {
    private fun versionTxtFile(project: Project): Path =
      project.rootDir.toPath().resolve("./version.txt")
  }

  @DisableCachingByDefault(because = "Version bumps cannot be cached")
  open class BumpVersionTask : DefaultTask() {
    @Option(
      option = "bumpToRelease",
      description = "Define whether to bump to a release version, defaults to snapshot release."
    )
    @Internal
    var bumpToRelease: Boolean = false

    @Option(
      option = "bumpType",
      description = "Defines which part of the version should be bumped, defaults to 'none'."
    )
    @Internal
    var bumpType: BumpType = BumpType.none

    @TaskAction
    fun bumpVersion() {
      val versionFile = versionTxtFile(project)
      val currentVersion = VersionTuple.fromFile(versionFile)

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
        Files.write(versionFile, finalVersion.toString().toByteArray(Charsets.UTF_8))
        logger.lifecycle("New version is $finalVersion.")
      } else {
        throw GradleException("Bump version tasks results in no change.")
      }
    }
  }

  enum class BumpType {
    none,
    patch,
    minor,
    major
  }
}
