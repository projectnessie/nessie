/*
 * Copyright (C) 2026 Dremio
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
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.gradle.testfixtures.ProjectBuilder
import org.gradle.testkit.runner.GradleRunner
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

class TestReleaseSupportPlugin {
  @Test
  fun taskWritesQualifiedReleaseVersion(@TempDir dir: Path) {
    val versionFile = dir.resolve("version.txt")
    Files.writeString(versionFile, "1.2.3-SNAPSHOT")

    val task = bumpTask(dir, versionFile)
    task.setBumpType("fix1")
    task.bumpToRelease = true

    task.bumpVersion()

    assertThat(Files.readString(versionFile)).isEqualTo("1.2.3-fix1")
  }

  @Test
  fun taskDoesNotMutateVersionFileWhenTransitionIsRejected(@TempDir dir: Path) {
    val versionFile = dir.resolve("version.txt")
    Files.writeString(versionFile, "1.2.3-SNAPSHOT")

    val task = bumpTask(dir, versionFile)
    task.setBumpType("fix1")

    assertThatThrownBy(task::bumpVersion)
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("A fix<N> bump type requires --bumpToRelease")
    assertThat(Files.readString(versionFile)).isEqualTo("1.2.3-SNAPSHOT")
  }

  @Test
  fun taskRejectsLowerDevelopmentVersionWithoutMutatingVersionFile(@TempDir dir: Path) {
    val versionFile = dir.resolve("version.txt")
    Files.writeString(versionFile, "1.2.3")

    val task = bumpTask(dir, versionFile)

    assertThatThrownBy(task::bumpVersion)
      .isInstanceOf(org.gradle.api.GradleException::class.java)
      .hasMessage("New version 1.2.3-SNAPSHOT would be lower than current version 1.2.3")
    assertThat(Files.readString(versionFile)).isEqualTo("1.2.3")
  }

  @Test
  fun taskReleasesCurrentPatchThenStartsTheFollowingDevelopmentPatch(@TempDir dir: Path) {
    val versionFile = dir.resolve("version.txt")
    Files.writeString(versionFile, "1.2.3-SNAPSHOT")

    val releaseTask = bumpTask(dir, versionFile)
    releaseTask.setBumpType("patch")
    releaseTask.bumpToRelease = true
    releaseTask.bumpVersion()
    assertThat(Files.readString(versionFile)).isEqualTo("1.2.3")

    val developmentTask = bumpTask(dir, versionFile)
    developmentTask.setBumpType("patch")
    developmentTask.bumpVersion()
    assertThat(Files.readString(versionFile)).isEqualTo("1.2.4-SNAPSHOT")
  }

  @Test
  fun commandLineOptionWritesQualifiedReleaseVersion(@TempDir dir: Path) {
    Files.writeString(dir.resolve("settings.gradle"), "rootProject.name = 'test-release-support'\n")
    val pluginClasses =
      ReleaseSupportPlugin::class.java.protectionDomain.codeSource.location.toURI().path
    Files.writeString(
      dir.resolve("build.gradle"),
      """
      buildscript {
        dependencies { classpath files('$pluginClasses') }
      }

      apply plugin: Class.forName('ReleaseSupportPlugin')
      """
        .trimIndent() + "\n",
    )
    val versionFile = dir.resolve("version.txt")
    Files.writeString(versionFile, "1.2.3-SNAPSHOT")

    GradleRunner.create()
      .withProjectDir(dir.toFile())
      .withArguments("bumpVersion", "--bumpType", "fix1", "--bumpToRelease")
      .build()

    assertThat(Files.readString(versionFile)).isEqualTo("1.2.3-fix1")
  }

  private fun bumpTask(
    projectDir: Path,
    versionFile: Path,
  ): ReleaseSupportPlugin.BumpVersionTask {
    val project = ProjectBuilder.builder().withProjectDir(projectDir.toFile()).build()
    project.pluginManager.apply(ReleaseSupportPlugin::class.java)
    return project.tasks
      .named("bumpVersion", ReleaseSupportPlugin.BumpVersionTask::class.java)
      .get()
      .also { it.versionFile.set(versionFile.toFile()) }
  }
}
