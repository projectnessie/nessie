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
import static org.assertj.core.api.InstanceOfAssertFactories.INTEGER;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestKey {
  @ParameterizedTest
  @MethodSource("compare")
  void compare(Key a, Key b, int expectedCompare) {
    assertThat(a)
        .describedAs("Compare of %s to %s expect %d", a, b, expectedCompare)
        .extracting(k -> Integer.signum(k.compareTo(b)))
        .asInstanceOf(INTEGER)
        .isEqualTo(expectedCompare);
  }

  static Stream<Arguments> compare() {
    return Stream.of(
        arguments(Key.of(), Key.of(), 0),
        arguments(Key.of(), Key.of(""), -1),
        arguments(Key.of(""), Key.of(), 1),
        arguments(Key.of(), Key.of("abcdef"), -1),
        arguments(Key.of("abcdef"), Key.of(), 1),
        arguments(Key.of("abcdef"), Key.of("0123", "123", "123"), 1),
        arguments(Key.of("abcdef", "abc", "abc"), Key.of("0123"), 1),
        arguments(Key.of(), Key.of("0123", "123", "123"), -1),
        arguments(Key.of("abcdef", "abc", "abc"), Key.of(), 1),
        arguments(Key.of("key.0"), Key.of("key.1"), -1),
        arguments(Key.of("key.1"), Key.of("key.0"), 1),
        arguments(Key.of("key.42"), Key.of("key.42"), 0),
        arguments(Key.of("key", "0"), Key.of("key", "1"), -1),
        arguments(Key.of("key", "1"), Key.of("key", "0"), 1),
        arguments(Key.of("key", "42"), Key.of("key", "42"), 0));
  }
}
