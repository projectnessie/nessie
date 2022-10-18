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
package org.projectnessie.versioned.storage.common.util;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.util.Ser.putVarInt;
import static org.projectnessie.versioned.storage.common.util.Util.asHex;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestSer {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> varInts() {
    return Stream.of(
        arguments(ByteBuffer.wrap(new byte[] {0}), 0, 1),
        arguments(ByteBuffer.wrap(new byte[] {65}), 65, 1),
        arguments(ByteBuffer.wrap(new byte[] {123}), 123, 1),
        arguments(ByteBuffer.wrap(new byte[] {(byte) 0x80, 1}), 1 << 7, 2),
        arguments(ByteBuffer.wrap(new byte[] {(byte) 0x80, 0x13}), 0x13 << 7, 2),
        arguments(ByteBuffer.wrap(new byte[] {(byte) 0xff, 0x13}), 0x7f | (0x13 << 7), 2),
        arguments(ByteBuffer.wrap(new byte[] {(byte) 0xff, 0x12}), 0x7f | (0x12 << 7), 2),
        arguments(
            ByteBuffer.wrap(new byte[] {(byte) 0xff, (byte) 0x80, 0x23}), 0x7f | (0x23 << 14), 3));
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("varInts")
  public void varIntLen(ByteBuffer src, int expected, int bytes) {
    int l = Ser.varIntLen(expected);
    soft.assertThat(l).isEqualTo(src.remaining());
  }

  @ParameterizedTest
  @MethodSource("varInts")
  public void readVarInt(ByteBuffer src, int expected, int bytes) {
    int p = src.position();
    int v = Ser.readVarInt(src);
    soft.assertThat(v).isEqualTo(expected);
    soft.assertThat(src.position() - p).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("varInts")
  public void readVarIntDirect(ByteBuffer src, int expected, int bytes) {
    ByteBuffer direct = ByteBuffer.allocateDirect(src.remaining());
    direct.put(src);
    direct.flip();

    int p = direct.position();
    int v = Ser.readVarInt(direct);
    soft.assertThat(v).isEqualTo(expected);
    soft.assertThat(direct.position() - p).isEqualTo(bytes);
  }

  @ParameterizedTest
  @MethodSource("varInts")
  public void writeVarInt(ByteBuffer src, int value, int bytes) {
    ByteBuffer buf = ByteBuffer.allocate(src.capacity());
    buf.position(src.position());
    putVarInt(buf, value);

    soft.assertThat(buf.position() - src.position()).isEqualTo(bytes);
    soft.assertThat(asHex(buf.duplicate().position(src.position()).limit(src.position() + bytes)))
        .isEqualTo(asHex(src.duplicate().limit(src.position() + bytes)));
  }

  @ParameterizedTest
  @MethodSource("varInts")
  public void writeVarIntDirect(ByteBuffer src, int value, int bytes) {
    ByteBuffer buf = ByteBuffer.allocateDirect(src.capacity());
    buf.position(src.position());
    putVarInt(buf, value);

    soft.assertThat(buf.position() - src.position()).isEqualTo(bytes);
    soft.assertThat(asHex(buf.duplicate().position(src.position()).limit(src.position() + bytes)))
        .isEqualTo(asHex(src.duplicate().limit(src.position() + bytes)));
  }
}
