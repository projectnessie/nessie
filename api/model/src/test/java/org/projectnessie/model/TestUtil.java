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
package org.projectnessie.model;

import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestUtil {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void fromPathString(String encoded, List<String> elements) {
    List<String> actualElements = Util.fromPathString(encoded);
    soft.assertThat(actualElements).containsExactlyElementsOf(elements);
    soft.assertThat(actualElements).containsExactlyElementsOf(Util.fromPathString(encoded));
  }

  @ParameterizedTest
  @MethodSource
  public void toPathString(String encoded, List<String> elements) {
    String actualEncoded = Util.toPathString(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded.replace('\u0000', '\u001d'));
    soft.assertThat(actualEncoded).isEqualTo(Util.toPathString(elements));
  }

  static Stream<Arguments> fromPathString() {
    return Stream.of(
        arguments("", List.of()),
        arguments("hello", List.of("hello")),
        arguments("hello..~", List.of("hello.")),
        arguments("..~hello..~", List.of(".hello.")),
        arguments("..~hello..~~", List.of(".hello.~")),
        arguments("..~hello..~~hello", List.of(".hello.~hello")),
        arguments("..~hello...~~hello", List.of(".hello", ".~hello")),
        arguments("..~hello..~.~hello", List.of(".hello.", "~hello")),
        arguments("..~hello..~~.~hello", List.of(".hello.~", "~hello")),
        arguments("..~~hello..~", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments("hello..~.world", List.of("hello.", "world")),
        arguments("hello...~world", List.of("hello", ".world")),
        arguments("he\u001dllo.wo\u001drld", List.of("he.llo", "wo.rld")),
        arguments("he\u0000llo.wo\u0000rld", List.of("he.llo", "wo.rld")),
        arguments("he..~llo.wo..~rld", List.of("he.llo", "wo.rld")));
  }

  static Stream<Arguments> toPathString() {
    return Stream.of(
        arguments("", List.of()),
        arguments("hello.world", List.of("hello", "world")),
        arguments("he\u001dllo.wo\u001drld", List.of("he.llo", "wo.rld")),
        arguments("he\u0000llo.wo\u0000rld", List.of("he.llo", "wo.rld")));
  }
}
