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

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatIllegalArgumentException
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource

class TestReleaseVersionTransition {
  @ParameterizedTest
  @MethodSource("transitions")
  fun appliesTransition(
    current: String,
    bumpType: String,
    bumpToRelease: Boolean,
    expected: String,
  ) {
    assertThat(
        ReleaseVersionTransition.apply(VersionTuple.create(current), bumpType, bumpToRelease)
      )
      .hasToString(expected)
  }

  @ParameterizedTest
  @MethodSource("invalidTransitions")
  fun rejectsInvalidTransition(current: String, bumpType: String, bumpToRelease: Boolean) {
    assertThatIllegalArgumentException().isThrownBy {
      ReleaseVersionTransition.apply(VersionTuple.create(current), bumpType, bumpToRelease)
    }
  }

  companion object {
    @JvmStatic
    fun transitions(): List<Arguments> =
      listOf(
        arguments("1.2.3-SNAPSHOT", "none", true, "1.2.3"),
        arguments("1.2.3-SNAPSHOT", "patch", true, "1.2.3"),
        arguments("1.2.3-SNAPSHOT", "minor", true, "1.3.0"),
        arguments("1.2.3-SNAPSHOT", "major", true, "2.0.0"),
        arguments("1.2.3-SNAPSHOT", "fix1", true, "1.2.3-fix1"),
        arguments("1.2.3", "patch", false, "1.2.4-SNAPSHOT"),
        arguments("1.2.3-fix1", "patch", false, "1.2.4-SNAPSHOT"),
        arguments("1.2.3", "minor", false, "1.3.0-SNAPSHOT"),
      )

    @JvmStatic
    fun invalidTransitions(): List<Arguments> =
      listOf(
        arguments("1.2.3-SNAPSHOT", "fix1", false),
        arguments("1.2.3", "fix1", true),
        arguments("1.2.3-SNAPSHOT", "fix0", true),
        arguments("1.2.3-SNAPSHOT", "fix01", true),
        arguments("1.2.3-SNAPSHOT", "fix", true),
        arguments("1.2.3-SNAPSHOT", "rc1", true),
        arguments("1.2.3-rc1", "patch", true),
      )
  }
}
