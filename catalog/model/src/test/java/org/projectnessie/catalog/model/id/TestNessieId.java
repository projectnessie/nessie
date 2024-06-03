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
package org.projectnessie.catalog.model.id;

import static java.lang.Long.parseUnsignedLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromByteAccessor;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromBytes;
import static org.projectnessie.catalog.model.id.NessieId.nessieIdFromLongs;

import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.stream.Stream;
import org.assertj.core.api.Assumptions;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestNessieId {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> hashOfString() {
    return Stream.of(
        arguments("", NessieIdEmpty.class),
        arguments("01", NessieIdGeneric.class),
        arguments("23", NessieIdGeneric.class),
        arguments("45", NessieIdGeneric.class),
        arguments("67", NessieIdGeneric.class),
        arguments("89", NessieIdGeneric.class),
        arguments("ab", NessieIdGeneric.class),
        arguments("cd", NessieIdGeneric.class),
        arguments("ef", NessieIdGeneric.class),
        arguments("AB", NessieIdGeneric.class),
        arguments("CD", NessieIdGeneric.class),
        arguments("EF", NessieIdGeneric.class),
        arguments("0123456789abcdef", NessieIdGeneric.class),
        arguments("0123456789abcdef", NessieIdGeneric.class),
        arguments(
            "0011223344556677" + "1213141516171819" + "2123242526272829" + "3132343536373839",
            NessieId256.class),
        arguments(
            "ffddbbaa88665544" + "9192939495969798" + "80776699deadbeef" + "cafebabe12345688",
            NessieId256.class));
  }

  @ParameterizedTest
  @MethodSource("hashOfString")
  void fromByteArray(String s, Class<? extends NessieId> type) {
    byte[] bytes = new byte[s.length() / 2];
    fromHex(s).get(bytes);

    NessieId h = nessieIdFromBytes(bytes);
    verify(s, type, h);
  }

  @ParameterizedTest
  @MethodSource("hashOfString")
  void fromByteAccessor(String s, Class<? extends NessieId> type) {
    byte[] bytes = new byte[s.length() / 2];
    fromHex(s).get(bytes);

    NessieId h = nessieIdFromByteAccessor(bytes.length, i -> bytes[i]);
    verify(s, type, h);
  }

  @ParameterizedTest
  @MethodSource("hashOfString")
  void fromLongs(String s, Class<? extends NessieId> type) {
    Assumptions.assumeThat(s.length()).isEqualTo(64);

    long l0 = parseUnsignedLong(s.substring(0, 16), 16);
    long l1 = parseUnsignedLong(s.substring(16, 32), 16);
    long l2 = parseUnsignedLong(s.substring(32, 48), 16);
    long l3 = parseUnsignedLong(s.substring(48), 16);

    NessieId h = nessieIdFromLongs(l0, l1, l2, l3);
    verify(s, type, h);
  }

  static Stream<Arguments> hashCodes() {
    return Stream.of(
        arguments("", 0),
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
    byte[] bytes = new byte[s.length() / 2];
    fromHex(s).get(bytes);

    assertThat(nessieIdFromBytes(bytes)).extracting(NessieId::hashCode).isEqualTo(expected);
  }

  private void verify(String s, Class<? extends NessieId> type, NessieId h) {
    soft.assertThat(h).isInstanceOf(type);
    soft.assertThat(h.size()).isEqualTo(s.length() / 2);
    soft.assertThat(h.idAsBytes()).hasSize(h.size()).isEqualTo(fromHex(s).array());
    soft.assertThat(h.idAsString()).isEqualTo(s.toLowerCase(Locale.US));
    soft.assertThat(nessieIdFromBytes(h.idAsBytes())).isEqualTo(h);
    soft.assertThat(nessieIdFromByteAccessor(h.size(), h::byteAt)).isEqualTo(h);
    soft.assertThat(nessieIdFromByteAccessor(h.size(), h::byteAt)).isEqualTo(h);
  }

  static ByteBuffer fromHex(String hex) {
    int l = hex.length() >> 1;
    ByteBuffer b = ByteBuffer.allocate(l);
    for (int p = 0; p < l; p++) {
      String hb =
          (p * 2 + 2 >= hex.length()) ? hex.substring(p * 2) : hex.substring(p * 2, p * 2 + 2);
      b.put((byte) Short.parseShort(hb, 16));
    }
    b.flip();
    return b;
  }
}
