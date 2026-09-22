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
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.UntrackedTask
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
        "Transitions the version to a release or next development version, see ' ./gradlew help --task :bumpVersion '."
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

  @UntrackedTask(because = "Version bumps intentionally mutate the current version file")
  abstract class BumpVersionTask : DefaultTask() {
    @get:Internal abstract val versionFile: RegularFileProperty

    @Option(
      option = "bumpToRelease",
      description = "Define whether to bump to a release version, defaults to snapshot release.",
    )
    @Internal
    var bumpToRelease: Boolean = false

    private var bumpType: String = "none"

    @Option(
      option = "bumpType",
      description =
        "Defines the version transition: none, patch, minor, major, or fix<N>; defaults to none.",
    )
    fun setBumpType(value: String) {
      bumpType = value
    }

    @TaskAction
    fun bumpVersion() {
      val versionFilePath = versionFile.get().asFile.toPath()
      val currentVersion = VersionTuple.fromFile(versionFilePath)

      logger.lifecycle("Current version is $currentVersion.")

      val finalVersion = ReleaseVersionTransition.apply(currentVersion, bumpType, bumpToRelease)

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
}

internal object ReleaseVersionTransition {
  private val fixQualifier = Regex("fix[1-9][0-9]*")

  fun apply(currentVersion: VersionTuple, bumpType: String, bumpToRelease: Boolean): VersionTuple {
    val transition = parseBumpType(bumpType)
    requireSupportedPrerelease(currentVersion)

    if (transition is BumpType.Fix) {
      require(bumpToRelease) { "A fix<N> bump type requires --bumpToRelease" }
      require(currentVersion.snapshot) {
        "A fix<N> bump type requires a -SNAPSHOT source version, but was $currentVersion"
      }
      return currentVersion.withPrerelease(transition.qualifier)
    }

    val nextVersion =
      when (transition) {
        BumpType.None -> currentVersion
        BumpType.Patch -> if (bumpToRelease) currentVersion else currentVersion.bumpPatch()
        BumpType.Minor -> currentVersion.bumpMinor()
        BumpType.Major -> currentVersion.bumpMajor()
        is BumpType.Fix -> error("Handled above")
      }

    return if (bumpToRelease) nextVersion.asRelease() else nextVersion.asSnapshot()
  }

  private fun parseBumpType(value: String): BumpType =
    when (value) {
      "none" -> BumpType.None
      "patch" -> BumpType.Patch
      "minor" -> BumpType.Minor
      "major" -> BumpType.Major
      else ->
        if (fixQualifier.matches(value)) {
          BumpType.Fix(value)
        } else {
          throw IllegalArgumentException(
            "Unsupported bump type '$value'; expected none, patch, minor, major, or fix<N>"
          )
        }
    }

  private fun requireSupportedPrerelease(version: VersionTuple) {
    val prerelease = version.prerelease
    require(prerelease == null || prerelease == "SNAPSHOT" || fixQualifier.matches(prerelease)) {
      "Unsupported prerelease '$prerelease' in version $version"
    }
  }

  private sealed class BumpType {
    data object None : BumpType()

    data object Patch : BumpType()

    data object Minor : BumpType()

    data object Major : BumpType()

    data class Fix(val qualifier: String) : BumpType()
  }
}
