/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.common.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestHex {

  @ParameterizedTest
  @MethodSource("stringToLong")
  void stringToLong(String s, int off, long expected) {
    assertThat(Hex.stringToLong(s, off)).isEqualTo(expected);
  }

  static Stream<Arguments> stringToLong() {
    return Stream.of(
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            0,
            0x0011223344556677L),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            16,
            0x1213141516171819L),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            32,
            0x2123242526272829L),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            48,
            0x3132343536373839L),
        //
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            0,
            0xffddbbaa88665544L),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            16,
            0x9192939495969798L),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            32,
            0x80776699deadbeefL),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            48,
            0xcafebabe12345688L));
  }
}
