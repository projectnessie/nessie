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
package org.projectnessie.versioned;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.protobuf.ByteString;
import java.util.Locale;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.Hash.GenericHash;
import org.projectnessie.versioned.Hash.Hash256;

public class TestHash {
  @SuppressWarnings("ConstantConditions")
  @Test
  void nullCheck() {
    assertThatThrownBy(() -> Hash.of((String) null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> Hash.of((ByteString) null)).isInstanceOf(NullPointerException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1", "123"})
  void illegalLengths(String s) {
    assertThatThrownBy(() -> Hash.of(s))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("length needs to be a multiple of two");
  }

  @Test
  void illegalChars() {
    assertThatThrownBy(() -> Hash.of("deadex"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Illegal hex character");
  }

  static Stream<Arguments> hashOfString() {
    return Stream.of(
        arguments("01", GenericHash.class),
        arguments("23", GenericHash.class),
        arguments("45", GenericHash.class),
        arguments("67", GenericHash.class),
        arguments("89", GenericHash.class),
        arguments("ab", GenericHash.class),
        arguments("cd", GenericHash.class),
        arguments("ef", GenericHash.class),
        arguments("AB", GenericHash.class),
        arguments("CD", GenericHash.class),
        arguments("EF", GenericHash.class),
        arguments("0123456789abcdef", GenericHash.class),
        arguments("0123456789abcdef", GenericHash.class),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            Hash256.class),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            Hash256.class));
  }

  @ParameterizedTest
  @MethodSource("hashOfString")
  void hashOfString(String s, Class<? extends Hash> type) {
    assertThat(Hash.of(s))
        .isInstanceOf(type)
        .satisfies(
            h -> assertThat(h.size()).isEqualTo(s.length() / 2),
            h -> assertThat(h.asBytes()).isEqualTo(ByteString.fromHex(s)),
            h -> assertThat(h.toString()).isEqualTo("Hash " + s.toLowerCase(Locale.US)),
            h -> assertThat(h).isEqualTo(Hash.of(ByteString.fromHex(s))));
  }

  static Stream<Arguments> hashCodes() {
    return Stream.of(
        arguments("01", 0x01000000),
        arguments("23", 0x23000000),
        arguments("45", 0x45000000),
        arguments("67", 0x67000000),
        arguments("89", 0x89000000),
        arguments("ab", 0xab000000),
        arguments("cd", 0xcd000000),
        arguments("ef", 0xef000000),
        arguments("AB", 0xab000000),
        arguments("CD", 0xcd000000),
        arguments("EF", 0xef000000),
        arguments("0123456789abcdef", 0x01234567),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            0x00112233),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            0xffddbbaa));
  }

  @ParameterizedTest
  @MethodSource("hashCodes")
  void hashCodes(String s, int expected) {
    assertThat(Hash.of(s)).extracting(Hash::hashCode).isEqualTo(expected);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "01",
        "23",
        "cd",
        "ef",
        "0123456789abcdef",
        "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
        "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688"
      })
  void nibbles(String s) {
    ByteString bytes = ByteString.fromHex(s);
    Hash h = Hash.of(s);

    assertThatThrownBy(() -> h.nibbleAt(-1)).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> h.nibbleAt(bytes.size() * 2)).isInstanceOf(RuntimeException.class);

    for (int i = 0; i < bytes.size(); i++) {
      int nib = i * 2;
      assertThat(h.nibbleAt(nib))
          .describedAs("nibble %s", nib)
          .isEqualTo((bytes.byteAt(i) >> 4) & 15);
      assertThat(h.nibbleAt(nib + 1))
          .describedAs("nibble %s", nib + 1)
          .isEqualTo(bytes.byteAt(i) & 15);
    }
  }

  @ParameterizedTest
  @MethodSource("bytesToLong")
  void bytesToLong(ByteString bytes, int off, long expected) {
    assertThat(Hash.bytesToLong(bytes, off)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("stringToLong")
  void stringToLong(String s, int off, long expected) {
    assertThat(Hash.stringToLong(s, off)).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("longToBytes")
  void longToBytes(long v, int off, byte[] expected) {
    byte[] arr = new byte[off + 8];
    Hash.longToBytes(arr, off, v);
    assertThat(arr).isEqualTo(expected);
  }

  static Stream<Arguments> longToBytes() {
    return Stream.of(
        arguments(0xcafebabe12345688L, 0, ByteString.fromHex("cafebabe12345688").toByteArray()),
        arguments(
            0xcafebabe12345688L,
            8,
            ByteString.fromHex("0000000000000000" + "cafebabe12345688").toByteArray()),
        arguments(
            0xcafebabe12345688L,
            16,
            ByteString.fromHex("0000000000000000" + "0000000000000000" + "cafebabe12345688")
                .toByteArray()),
        arguments(
            0xcafebabe12345688L,
            24,
            ByteString.fromHex(
                    "0000000000000000"
                        + "0000000000000000"
                        + "0000000000000000"
                        + "cafebabe12345688")
                .toByteArray()));
  }

  static Stream<Arguments> bytesToLong() {
    return stringToLong()
        .map(Arguments::get)
        .map(
            args ->
                Arguments.of(
                    ByteString.fromHex((String) args[0]), ((Integer) args[1]) / 2, args[2]));
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
