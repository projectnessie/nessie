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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestJdkSpecifics {
  @InjectSoftAssertions SoftAssertions soft;

  static Stream<Arguments> mismatch() {
    return Stream.of(
        arguments("", "", -1),
        arguments("foo", "foo", -1),
        arguments("a", "b", 0),
        arguments("meeps", "meefs", 3));
  }

  @ParameterizedTest
  @MethodSource("mismatch")
  public void mismatch(String s1, String s2, int expected) {
    Function<String, ByteBuffer> heap =
        s -> {
          byte[] byteArray = s.getBytes(StandardCharsets.UTF_8);
          return ByteBuffer.allocate(byteArray.length).put(byteArray).flip();
        };
    Function<String, ByteBuffer> direct =
        s -> {
          byte[] byteArray = s.getBytes(StandardCharsets.UTF_8);
          return ByteBuffer.allocateDirect(byteArray.length).put(byteArray).flip();
        };

    ByteBuffer heap1 = heap.apply(s1);
    ByteBuffer heap2 = heap.apply(s2);
    ByteBuffer direct1 = direct.apply(s1);
    ByteBuffer direct2 = direct.apply(s2);

    soft.assertThat(new JdkSpecifics.Java8().mismatch(heap1, heap2))
        .isEqualTo(expected)
        .isEqualTo(new JdkSpecifics.Java11().mismatch(heap1, heap2));
    soft.assertThat(new JdkSpecifics.Java8().mismatch(direct1, heap2))
        .isEqualTo(expected)
        .isEqualTo(new JdkSpecifics.Java11().mismatch(direct1, heap2));
    soft.assertThat(new JdkSpecifics.Java8().mismatch(heap1, direct2))
        .isEqualTo(expected)
        .isEqualTo(new JdkSpecifics.Java11().mismatch(heap1, direct2));
    soft.assertThat(new JdkSpecifics.Java8().mismatch(direct1, direct2))
        .isEqualTo(expected)
        .isEqualTo(new JdkSpecifics.Java11().mismatch(direct1, direct2));
  }
}
