/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.HashSet;
import org.junit.jupiter.api.Test;

class TestVersion {
  @Test
  void version() {
    assertThat(Version.parse("0.0.0").toString()).isEqualTo("0.0.0");
    assertThat(Version.parse("0.0").toString()).isEqualTo("0.0.0");
    assertThat(Version.parse("0").toString()).isEqualTo("0.0.0");
    assertThat(Version.parse("0.0.0-SNAPSHOT").toString()).isEqualTo("0.0.0-SNAPSHOT");
    assertThat(Version.parse("0.0-SNAPSHOT").toString()).isEqualTo("0.0.0-SNAPSHOT");
    assertThat(Version.parse("0-SNAPSHOT").toString()).isEqualTo("0.0.0-SNAPSHOT");

    assertThat(Version.parse("1.2.3").toString()).isEqualTo("1.2.3");
    assertThat(Version.parse("1.2").toString()).isEqualTo("1.2.0");
    assertThat(Version.parse("1").toString()).isEqualTo("1.0.0");
    assertThat(Version.parse("1.2.3-SNAPSHOT").toString()).isEqualTo("1.2.3-SNAPSHOT");
    assertThat(Version.parse("1.2-SNAPSHOT").toString()).isEqualTo("1.2.0-SNAPSHOT");
    assertThat(Version.parse("1-SNAPSHOT").toString()).isEqualTo("1.0.0-SNAPSHOT");

    assertThat(Version.parse("0.1.3-SNAPSHOT")).extracting("snapshot").isEqualTo(true);
    assertThat(Version.parse("0.1.3-SNAPSHOT").removeSnapshot())
        .extracting("snapshot")
        .isEqualTo(false);
    assertThat(Version.parse("0.1.3").removeSnapshot()).extracting("snapshot").isEqualTo(false);
    assertThat(Version.parse("0.1.3-SNAPSHOT").removeSnapshot()).isEqualTo(Version.parse("0.1.3"));
    assertThat(Version.parse("0.1.3")).isEqualTo(Version.parse("0.1.3-SNAPSHOT").removeSnapshot());

    assertThat(Version.parse("1.2.3")).isEqualTo(new Version(1, 2, 3, false));
    assertThat(Version.parse("1.2")).isEqualTo(new Version(1, 2, 0, false));
    assertThat(Version.parse("1")).isEqualTo(new Version(1, 0, 0, false));
    assertThat(Version.parse("1.2.3-SNAPSHOT")).isEqualTo(new Version(1, 2, 3, true));
    assertThat(Version.parse("1.2-SNAPSHOT")).isEqualTo(new Version(1, 2, 0, true));
    assertThat(Version.parse("1-SNAPSHOT")).isEqualTo(new Version(1, 0, 0, true));

    assertThat(Version.parse("1.2.3")).isGreaterThan(Version.parse("1"));
    assertThat(Version.parse("1.2.3")).isGreaterThan(Version.parse("1.2"));
    assertThat(Version.parse("1.2.3")).isGreaterThan(Version.parse("1.2.3-SNAPSHOT"));
    assertThat(Version.parse("1.2.3-SNAPSHOT")).isGreaterThan(Version.parse("1"));
    assertThat(Version.parse("1.2.3-SNAPSHOT")).isGreaterThan(Version.parse("1.2"));
    assertThat(Version.parse("1.2.3-SNAPSHOT"))
        .isEqualByComparingTo(Version.parse("1.2.3-SNAPSHOT"));

    assertThat(Version.parse("1.2.3")).isGreaterThanOrEqualTo(Version.parse("1.2.3-SNAPSHOT"));

    assertThat(
            new HashSet<>(
                Arrays.asList(
                    Version.parse("1"),
                    Version.parse("1.0"),
                    Version.parse("1.0.0"),
                    Version.parse("1.2"),
                    Version.parse("1.2.0"),
                    Version.parse("1.2.3"),
                    Version.parse("1-SNAPSHOT"),
                    Version.parse("1.0-SNAPSHOT"),
                    Version.parse("1.0.0-SNAPSHOT"),
                    Version.parse("1.2-SNAPSHOT"),
                    Version.parse("1.2.3-SNAPSHOT"))))
        .containsAll(
            Arrays.asList(
                Version.parse("1"),
                Version.parse("1.2"),
                Version.parse("1.2.3"),
                Version.parse("1-SNAPSHOT"),
                Version.parse("1.2-SNAPSHOT"),
                Version.parse("1.2.3-SNAPSHOT")))
        .hasSize(6);

    assertThatThrownBy(() -> Version.parse(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("null version argument");
    assertThatThrownBy(() -> Version.parse("x.y.z"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Not a valid version string: x.y.z");
  }
}
