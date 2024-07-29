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
package org.projectnessie.catalog.service.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestTimestampParser {
  @ParameterizedTest
  @MethodSource
  public void parse(String input, String expected) {
    assertThat(TimestampParser.timestampToNessie(input)).isEqualTo(expected);
  }

  static Stream<Arguments> parse() {
    return Stream.of(
        arguments("12345", "*12345"),
        arguments("2024-07-29T13:14:15Z", "*2024-07-29T13:14:15Z"),
        arguments("2024-07-29T13:14:15-09:00", "*2024-07-29T22:14:15Z"),
        arguments("2024-07-29T13:14:15+02:00", "*2024-07-29T11:14:15Z"));
  }
}
