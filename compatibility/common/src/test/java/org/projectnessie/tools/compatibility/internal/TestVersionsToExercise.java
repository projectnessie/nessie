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
package org.projectnessie.tools.compatibility.internal;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.tools.compatibility.api.Version;

@ExtendWith(SoftAssertionsExtension.class)
class TestVersionsToExercise {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void noVersions() {
    soft.assertThatThrownBy(() -> VersionsToExercise.versionsFromValue(null))
        .isInstanceOf(IllegalArgumentException.class);
    soft.assertThatThrownBy(() -> VersionsToExercise.versionsFromValue(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"1,3,2.4,current,2.2", "3,2.4,1,2.2,current"})
  void sorting(String s) {
    soft.assertThat(VersionsToExercise.versionsFromValue(s))
        .containsExactly(
            Version.parseVersion("1"),
            Version.parseVersion("2.2"),
            Version.parseVersion("2.4"),
            Version.parseVersion("3"),
            Version.parseVersion("current"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "-1", "0.-1", "abc", "-1.0", "-1.1", "1.-1", "1.", ".1", ".1.", ".1.1", "1.1.", "1 .1",
        "1. 1"
      })
  void parseIllegal(String s) {
    soft.assertThatThrownBy(() -> VersionsToExercise.versionsFromValue(s))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid version number part");
  }
}
