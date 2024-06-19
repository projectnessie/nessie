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
package org.projectnessie.client.rest.io;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.base.Strings;
import java.io.ByteArrayInputStream;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestCapturingInputStream {

  @ParameterizedTest
  @MethodSource("capturingInputStream")
  void captured(int blockSize, byte[] source, String expected) throws Exception {
    CapturingInputStream capturing = new CapturingInputStream(new ByteArrayInputStream(source));
    if (blockSize == 0) {
      while (capturing.read() >= 0) {
        // noop
      }
    } else {
      byte[] tmp = new byte[blockSize * 2];
      while (capturing.read(tmp, 8, blockSize) >= 0) {
        // noop
      }
    }
    if (expected.length() > CapturingInputStream.CAPTURE_LEN) {
      expected = expected.substring(0, CapturingInputStream.CAPTURE_LEN);
    }
    assertThat(capturing.captured()).isEqualTo(expected);
  }

  @ParameterizedTest
  @MethodSource("capturingInputStream")
  void capture(int ignored, byte[] source, String expected) {
    CapturingInputStream capturing = new CapturingInputStream(new ByteArrayInputStream(source));
    if (expected.length() > CapturingInputStream.CAPTURE_LEN) {
      expected = expected.substring(0, CapturingInputStream.CAPTURE_LEN);
    }
    assertThat(capturing.capture()).isEqualTo(expected);
  }

  static Stream<Arguments> capturingInputStream() {
    String longString = Strings.repeat("hello world", 500);
    return Stream.of(
        arguments(0, new byte[0], ""),
        arguments(16, new byte[0], ""),
        arguments(0, "hello world".getBytes(UTF_8), "hello world"),
        arguments(16, "hello world".getBytes(UTF_8), "hello world"),
        arguments(0, longString.getBytes(UTF_8), longString),
        arguments(16, longString.getBytes(UTF_8), longString));
  }
}
