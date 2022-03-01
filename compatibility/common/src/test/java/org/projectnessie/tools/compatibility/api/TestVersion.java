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
package org.projectnessie.tools.compatibility.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestVersion {
  @Test
  void parseNPE() {
    assertThatThrownBy(() -> Version.parseVersion(null)).isInstanceOf(NullPointerException.class);
  }

  @Test
  void current() {
    assertThat(Version.parseVersion(Version.CURRENT_STRING)).isSameAs(Version.CURRENT);
  }

  @Test
  void notCurrent() {
    assertThat(Version.parseVersion(Version.NOT_CURRENT_STRING)).isSameAs(Version.NOT_CURRENT);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "-1", "0.-1", "", "abc", "-1.0", "-1.1", "1.-1", "1.", ".1", ".1.", ".1.1", "1.1.", " 1",
        "1 ", "1 .1", "1. 1"
      })
  void parseIllegal(String s) {
    assertThatThrownBy(() -> Version.parseVersion(s))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid version number part");
  }

  @ParameterizedTest
  @ValueSource(strings = {"1", "1.2", "0.1"})
  void parseToString(String s) {
    assertThat(Version.parseVersion(s)).extracting(Version::toString).isEqualTo(s);
  }

  @Test
  void compare() {
    assertThat(Version.parseVersion("1.0.0"))
        .isEqualTo(Version.parseVersion("1.0.0"))
        .isNotEqualTo(Version.parseVersion("1.0"))
        .isNotEqualTo(Version.parseVersion("1"))
        .isEqualByComparingTo(Version.parseVersion("1"))
        .isEqualByComparingTo(Version.parseVersion("1.0"))
        .isEqualByComparingTo(Version.parseVersion("1.0.0"))
        .isLessThan(Version.CURRENT)
        .isLessThan(Version.NOT_CURRENT)
        .isLessThan(Version.parseVersion("1.0.1"))
        .isLessThan(Version.parseVersion("1.1"))
        .isLessThan(Version.parseVersion("1.1.0"))
        .isGreaterThan(Version.parseVersion("0.0.1"))
        .isGreaterThan(Version.parseVersion("0"))
        .isGreaterThan(Version.parseVersion("0.1"))
        .isGreaterThan(Version.parseVersion("0.99999"))
        .isGreaterThan(Version.parseVersion("0.1.0"));
  }

  @Test
  public void isGreaterThan() {
    assertThat(Version.parseVersion("1.0.0").isGreaterThanOrEqual(Version.parseVersion("1.0.0")))
        .isTrue();
    assertThat(Version.parseVersion("1.0.0").isGreaterThan(Version.parseVersion("0.9.9"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isGreaterThan(Version.parseVersion("0.9999")))
        .isTrue();
    assertThat(Version.parseVersion("1.0.0").isGreaterThan(Version.parseVersion("1.0.0")))
        .isFalse();
  }

  @Test
  public void isLessThan() {
    assertThat(Version.parseVersion("1.0.0").isLessThanOrEqual(Version.parseVersion("1.0.0")))
        .isTrue();
    assertThat(Version.parseVersion("1.0.0").isLessThan(Version.parseVersion("1.0.1"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isLessThan(Version.parseVersion("1.1"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isLessThan(Version.parseVersion("1.0.1"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isLessThan(Version.parseVersion("1.0.0"))).isFalse();
  }

  @Test
  public void isSame() {
    assertThat(Version.parseVersion("1.0").isSame(Version.parseVersion("1.0.0"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isSame(Version.parseVersion("1.0.0"))).isTrue();
    assertThat(Version.parseVersion("1.0.0").isSame(Version.parseVersion("1.0.1"))).isFalse();
    assertThat(Version.parseVersion("1.0.0").isSame(Version.parseVersion("0.9.9"))).isFalse();
  }
}
