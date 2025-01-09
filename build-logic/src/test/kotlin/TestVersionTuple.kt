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
  @ValueSource(strings = ["1", "v1", "1.0", "1.b"])
  fun invalids(ver: String) {
    assertThatThrownBy { VersionTuple.create(ver) }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessageEndingWith("is not a valid version string")
  }

  @Test
  fun invalidPrerelease() {
    assertThatThrownBy { VersionTuple.create("1.2.3-FOOBAR") }
      .isInstanceOf(IllegalArgumentException::class.java)
      .hasMessage("Only SNAPSHOT prerelease supported, but FOOBAR != SNAPSHOT")
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
    assertThat(VersionTuple(1, 2, 3, false).asSnapshot()).isEqualTo(VersionTuple(1, 2, 3, true))
    assertThat(VersionTuple(1, 2, 3, true).asSnapshot()).isEqualTo(VersionTuple(1, 2, 3, true))
  }

  @Test
  fun asRelease() {
    assertThat(VersionTuple(1, 2, 3, false).asRelease()).isEqualTo(VersionTuple(1, 2, 3, false))
    assertThat(VersionTuple(1, 2, 3, true).asRelease()).isEqualTo(VersionTuple(1, 2, 3, false))
  }

  @Test
  fun bumpPatch() {
    assertThat(VersionTuple(1, 2, 3, false).bumpPatch()).isEqualTo(VersionTuple(1, 2, 4, false))
    assertThat(VersionTuple(1, 2, 3, true).bumpPatch()).isEqualTo(VersionTuple(1, 2, 4, false))
  }

  @Test
  fun bumpMinor() {
    assertThat(VersionTuple(1, 2, 3, false).bumpMinor()).isEqualTo(VersionTuple(1, 3, 0, false))
    assertThat(VersionTuple(1, 2, 3, true).bumpMinor()).isEqualTo(VersionTuple(1, 3, 0, false))
  }

  @Test
  fun bumpMajor() {
    assertThat(VersionTuple(1, 2, 3, false).bumpMajor()).isEqualTo(VersionTuple(2, 0, 0, false))
    assertThat(VersionTuple(1, 2, 3, true).bumpMajor()).isEqualTo(VersionTuple(2, 0, 0, false))
  }

  @Test
  fun fromFile(@TempDir dir: Path) {
    val file = dir.resolve("ver.txt")
    Files.writeString(file, "1.2.3")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, false))
    Files.writeString(file, "1.2.3-SNAPSHOT")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, true))
    Files.writeString(file, "1.2.3\n")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, false))
    Files.writeString(file, "1.2.3-SNAPSHOT\n")
    assertThat(VersionTuple.fromFile(file)).isEqualTo(VersionTuple(1, 2, 3, true))
  }

  @Test
  fun validVersion() {
    assertThat(VersionTuple.create("1.2.3"))
      .extracting(
        VersionTuple::major,
        VersionTuple::minor,
        VersionTuple::patch,
        VersionTuple::snapshot,
        VersionTuple::toString,
      )
      .containsExactly(1, 2, 3, false, "1.2.3")
  }

  @Test
  fun validSnapshotVersion() {
    assertThat(VersionTuple.create("1.2.3-SNAPSHOT"))
      .extracting(
        VersionTuple::major,
        VersionTuple::minor,
        VersionTuple::patch,
        VersionTuple::snapshot,
        VersionTuple::toString,
      )
      .containsExactly(1, 2, 3, true, "1.2.3-SNAPSHOT")
  }

  @Test
  fun equals() {
    assertThat(VersionTuple.create("1.2.3")).isEqualTo(VersionTuple(1, 2, 3, false))
    assertThat(VersionTuple.create("1.2.3-SNAPSHOT")).isEqualTo(VersionTuple(1, 2, 3, true))
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
        arguments(VersionTuple.create("2.2.4"), VersionTuple(1, 2, 3, false), 1),
        arguments(VersionTuple.create("2.2.4"), VersionTuple(1, 2, 3, true), 1),
        arguments(VersionTuple.create("1.3.4"), VersionTuple(1, 2, 3, false), 1),
        arguments(VersionTuple.create("1.3.4"), VersionTuple(1, 2, 3, true), 1),
        arguments(VersionTuple.create("1.2.3"), VersionTuple(1, 2, 4, false), -1),
        arguments(VersionTuple.create("1.2.4"), VersionTuple(1, 2, 3, false), 1),
        arguments(VersionTuple.create("1.2.4"), VersionTuple(1, 2, 3, true), 1),
        arguments(VersionTuple.create("1.2.4-SNAPSHOT"), VersionTuple(1, 2, 3, true), 1),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple(1, 2, 4, true), -1),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple(1, 2, 4, true), -1),
        arguments(VersionTuple.create("1.2.3"), VersionTuple(1, 2, 3, false), 0),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple(1, 2, 3, true), 0),
        arguments(VersionTuple.create("1.2.3-SNAPSHOT"), VersionTuple(1, 2, 3, false), -1),
      )
  }
}
