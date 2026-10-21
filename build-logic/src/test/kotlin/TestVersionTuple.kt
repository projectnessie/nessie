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

import java.lang.IllegalArgumentException
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource

class TestVersionTuple {
  @ParameterizedTest
  @ValueSource(strings = ["1", "v1", "1.0", "1.b", "1.2.3-01", "1.2.3-fix.01"])
  fun invalids(ver: String) {
    assertThatThrownBy { VersionTuple.create(ver) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageEndingWith("is not a valid version string")
  }

  @ParameterizedTest
  @ValueSource(strings = ["1.2.3-SNAPSHOT+BUILDMETA", "1.2.3+BUILDMETA"])
  fun invalidBuildMetadata(ver: String) {
    assertThatThrownBy { VersionTuple.create(ver) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Build metadata not supported")
  }

  @Test
  fun asSnapshot() {
    assertThat(VersionTuple(1, 2, 3).asSnapshot()).isEqualTo(VersionTuple(1, 2, 3, "SNAPSHOT"))
    assertThat(VersionTuple(1, 2, 3, "fix1").asSnapshot())
      .isEqualTo(VersionTuple(1, 2, 3, "SNAPSHOT"))
  }

  @Test
  fun asRelease() {
    assertThat(VersionTuple(1, 2, 3).asRelease()).isEqualTo(VersionTuple(1, 2, 3))
    assertThat(VersionTuple(1, 2, 3, "SNAPSHOT").asRelease()).isEqualTo(VersionTuple(1, 2, 3))
    assertThat(VersionTuple(1, 2, 3, "fix1").asRelease()).isEqualTo(VersionTuple(1, 2, 3))
  }

  @Test
  fun bumpPatch() {
    assertThat(VersionTuple(1, 2, 3).bumpPatch()).isEqualTo(VersionTuple(1, 2, 4))
    assertThat(VersionTuple(1, 2, 3, "SNAPSHOT").bumpPatch()).isEqualTo(VersionTuple(1, 2, 4))
    assertThat(VersionTuple(1, 2, 3, "fix1").bumpPatch()).isEqualTo(VersionTuple(1, 2, 4))
  }

  @Test
  fun bumpMinor() {
    assertThat(VersionTuple(1, 2, 3).bumpMinor()).isEqualTo(VersionTuple(1, 3, 0))
    assertThat(VersionTuple(1, 2, 3, "SNAPSHOT").bumpMinor()).isEqualTo(VersionTuple(1, 3, 0))
  }

  @Test
  fun bumpMajor() {
    assertThat(VersionTuple(1, 2, 3).bumpMajor()).isEqualTo(VersionTuple(2, 0, 0))
    assertThat(VersionTuple(1, 2, 3, "SNAPSHOT").bumpMajor()).isEqualTo(VersionTuple(2, 0, 0))
  }

  @Test
  fun fromFile(@TempDir dir: Path) {
    val file = dir.resolve("ver.txt")
    Files.writeString(file, "1.2.3")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3))
    Files.writeString(file, "1.2.3-SNAPSHOT")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, "SNAPSHOT"))
    Files.writeString(file, "1.2.3-fix1\n")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, "fix1"))
  }

  @Test
  fun validVersion() {
    assertThat(VersionTuple.create("1.2.3"))
      .extracting(
        VersionTuple::major,
        VersionTuple::minor,
        VersionTuple::patch,
        VersionTuple::prerelease,
        VersionTuple::snapshot,
        VersionTuple::toString,
      )
      .containsExactly(1, 2, 3, null, false, "1.2.3")
  }

  @Test
  fun validPrereleaseVersions() {
    assertThat(VersionTuple.create("1.2.3-SNAPSHOT")).isEqualTo(VersionTuple(1, 2, 3, "SNAPSHOT"))
    assertThat(VersionTuple.create("1.2.3-fix1")).isEqualTo(VersionTuple(1, 2, 3, "fix1"))
    assertThat(VersionTuple.create("1.2.3-alpha.1")).isEqualTo(VersionTuple(1, 2, 3, "alpha.1"))
  }

  @ParameterizedTest
  @MethodSource("compare")
  fun compare(ver1: VersionTuple, ver2: VersionTuple, expected: Int) {
    when (expected) {
      1 -> assertThat(ver1).isGreaterThan(ver2)
      -1 -> assertThat(ver1).isLessThan(ver2)
      0 -> assertThat(ver1).isEqualByComparingTo(ver2)
      else -> fail()
    }
  }

  companion object {
    @JvmStatic
    fun compare(): List<Arguments> =
      listOf(
        arguments(VersionTuple.create("2.2.4"), VersionTuple.create("1.2.3"), 1),
        arguments(VersionTuple.create("1.3.4"), VersionTuple.create("1.2.3"), 1),
        arguments(VersionTuple.create("1.2.3"), VersionTuple.create("1.2.4"), -1),
        arguments(VersionTuple.create("1.2.4-SNAPSHOT"), VersionTuple.create("1.2.3-SNAPSHOT"), 1),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple.create("1.2.3"), -1),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple.create("1.2.3-fix1"), -1),
        arguments(VersionTuple.create("1.2.3-fix1"), VersionTuple.create("1.2.3"), -1),
        arguments(
          VersionTuple.create("1.2.3-alpha.1"),
          VersionTuple.create("1.2.3-alpha.beta"),
          -1,
        ),
        arguments(VersionTuple.create("1.2.3-alpha.1"), VersionTuple.create("1.2.3-alpha.2"), -1),
        arguments(VersionTuple.create("1.2.3-alpha"), VersionTuple.create("1.2.3-alpha.1"), -1),
        arguments(VersionTuple.create("1.2.3-1"), VersionTuple.create("1.2.3-alpha"), -1),
        arguments(VersionTuple.create("1.2.3-fix10"), VersionTuple.create("1.2.3-fix2"), -1),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple.create("1.2.3-SNAPSHOT"), 0),
      )
  }
}
