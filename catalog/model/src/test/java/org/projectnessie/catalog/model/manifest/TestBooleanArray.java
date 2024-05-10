/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.model.manifest;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestBooleanArray {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void encodeDecodeBoolean(Boolean v, byte encoded) {
    soft.assertThat(BooleanArray.encodeBoolean(v)).isEqualTo(encoded);
    soft.assertThat(BooleanArray.decodeBoolean(encoded)).isEqualTo(v);
  }

  static Stream<Arguments> encodeDecodeBoolean() {
    return Stream.of(
        arguments(null, (byte) 0), arguments(FALSE, (byte) 1), arguments(TRUE, (byte) 2));
  }

  @ParameterizedTest
  @MethodSource
  public void encodeDecodeForIndex(Boolean v, int index, byte encoded) {
    soft.assertThat(BooleanArray.encodedForIndex(index, v)).isEqualTo(encoded);
    soft.assertThat(BooleanArray.decodedForIndex(index, encoded)).isEqualTo(v);
  }

  static Stream<Arguments> encodeDecodeForIndex() {
    return Stream.of(
        arguments(null, 0, (byte) 0),
        arguments(FALSE, 0, (byte) 0x1),
        arguments(TRUE, 0, (byte) 0x2),
        arguments(null, 1, (byte) 0),
        arguments(FALSE, 1, (byte) 0x4),
        arguments(TRUE, 1, (byte) 0x8),
        arguments(null, 2, (byte) 0),
        arguments(FALSE, 2, (byte) 0x10),
        arguments(TRUE, 2, (byte) 0x20),
        arguments(null, 3, (byte) 0),
        arguments(FALSE, 3, (byte) 0x40),
        arguments(TRUE, 3, (byte) 0x80),
        //
        arguments(null, 4, (byte) 0),
        arguments(FALSE, 4, (byte) 0x1),
        arguments(TRUE, 4, (byte) 0x2),
        arguments(null, 5, (byte) 0),
        arguments(FALSE, 5, (byte) 0x4),
        arguments(TRUE, 5, (byte) 0x8),
        arguments(null, 6, (byte) 0),
        arguments(FALSE, 6, (byte) 0x10),
        arguments(TRUE, 6, (byte) 0x20),
        arguments(null, 7, (byte) 0),
        arguments(FALSE, 7, (byte) 0x40),
        arguments(TRUE, 7, (byte) 0x80));
  }

  @ParameterizedTest
  @MethodSource
  public void maskForIndex(int index, byte mask) {
    soft.assertThat(BooleanArray.maskForIndex(index)).isEqualTo(mask);
  }

  static Stream<Arguments> maskForIndex() {
    return Stream.of(
        arguments(0, (byte) 0x03),
        arguments(1, (byte) 0x0c),
        arguments(2, (byte) 0x30),
        arguments(3, (byte) 0xc0),
        arguments(4, (byte) 0x03),
        arguments(5, (byte) 0x0c),
        arguments(6, (byte) 0x30),
        arguments(7, (byte) 0xc0),
        arguments(128, (byte) 0x03),
        arguments(129, (byte) 0x0c),
        arguments(130, (byte) 0x30),
        arguments(131, (byte) 0xc0));
  }

  @ParameterizedTest
  @MethodSource
  public void shiftForIndex(int index, int shift) {
    soft.assertThat(BooleanArray.shiftForIndex(index)).isEqualTo(shift);
  }

  static Stream<Arguments> shiftForIndex() {
    return Stream.of(
        arguments(0, 0),
        arguments(1, 2),
        arguments(2, 4),
        arguments(3, 6),
        arguments(4, 0),
        arguments(5, 2),
        arguments(6, 4),
        arguments(7, 6),
        arguments(128, 0),
        arguments(129, 2),
        arguments(130, 4),
        arguments(131, 6));
  }

  @ParameterizedTest
  @MethodSource
  public void arrayIndex(int index, int dataIndex) {
    soft.assertThat(BooleanArray.arrayIndex(index)).isEqualTo(dataIndex);
  }

  static Stream<Arguments> arrayIndex() {
    return Stream.of(
        arguments(0, 0),
        arguments(1, 0),
        arguments(2, 0),
        arguments(3, 0),
        arguments(4, 1),
        arguments(5, 1),
        arguments(6, 1),
        arguments(7, 1),
        arguments(8, 2),
        arguments(9, 2),
        arguments(10, 2),
        arguments(11, 2),
        arguments(127, 31),
        arguments(128, 32),
        arguments(129, 32),
        arguments(130, 32),
        arguments(131, 32),
        arguments(132, 33));
  }

  @ParameterizedTest
  @MethodSource
  public void validate(List<Boolean> reference) {
    BooleanArray array = new BooleanArray(reference.size());
    for (int i = 0; i < reference.size(); i++) {
      array.set(i, reference.get(i));
    }

    List<Boolean> extracted =
        IntStream.range(0, reference.size()).mapToObj(array::get).collect(Collectors.toList());
    soft.assertThat(extracted).containsExactlyElementsOf(reference);

    // reverse

    for (int i = 0; i < reference.size(); i++) {
      array.set(reference.size() - i - 1, reference.get(i));
    }
    extracted =
        IntStream.range(0, reference.size())
            .mapToObj(i -> array.get(reference.size() - i - 1))
            .collect(Collectors.toList());
    soft.assertThat(extracted).containsExactlyElementsOf(reference);
  }

  static List<List<Boolean>> validate() {

    List<Boolean> possibleValues = new ArrayList<>();
    possibleValues.add(null);
    possibleValues.add(TRUE);
    possibleValues.add(FALSE);

    List<List<Boolean>> result = new ArrayList<>();
    // validate[] test index #0
    result.add(emptyList());

    // all possible combinations for the first byte
    int lastStartIdx = 0;
    int maxLength = 4;
    // validate[] test index #1 to #121 (including)
    for (int i = 0; i < maxLength; i++) {
      int s = result.size();
      for (int p = lastStartIdx; p < s; p++) {
        for (Boolean v : possibleValues) {
          List<Boolean> current = new ArrayList<>(result.get(p));
          current.add(v);
          result.add(current);
        }
      }
      lastStartIdx = s;
    }

    // repeat upper pattern list for 2nd byte (index #122 to #241)
    int l = result.size();
    for (int i = 1; i < l; i++) {
      List<Boolean> ref = result.get(i);
      List<Boolean> current = new ArrayList<>(4 + ref.size());
      for (int i1 = 0; i1 < 4; i1++) {
        current.add(null);
      }
      current.addAll(ref);
      result.add(current);
    }

    // some patterns
    List<List<Boolean>> patterns = new ArrayList<>();
    patterns.add(Arrays.asList(null, true, false, null, true, false, true, false));
    patterns.add(Arrays.asList(null, true, false, null, true, false, true, false));
    patterns.add(Arrays.asList(true, false, null, true, false, true, false, null));
    patterns.add(Arrays.asList(false, null, true, false, true, false, null, true));

    // some patterns for 5 bytes (test index #242 to #245)
    for (List<Boolean> p : patterns) {
      List<Boolean> current = new ArrayList<>(5 * 8);
      for (int i = 0; i < 5; i++) {
        current.addAll(p);
      }
      result.add(current);
    }

    return result;
  }
}
