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

import static java.lang.String.format;
import static org.eclipse.jetty.http.UriCompliance.Violation.ILLEGAL_PATH_CHARACTERS;
import static org.eclipse.jetty.http.UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.UriCompliance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestContentKeyEncoding {
  @InjectSoftAssertions protected SoftAssertions soft;

  @ParameterizedTest
  @MethodSource
  public void fromPathString(
      String encoded,
      List<String> elements,
      Collection<UriCompliance.Violation> expectedViolations) {
    List<String> actualElements = Util.fromPathString(encoded);
    soft.assertThat(actualElements).containsExactlyElementsOf(elements);
    soft.assertThat(actualElements).containsExactlyElementsOf(Util.fromPathString(encoded));

    HttpURI uri = HttpURI.from(format("http://hostname/%s", encoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-beginning: %s", encoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    uri = HttpURI.from(format("http://hostname/foo/%s", encoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end: %s", encoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    if (!encoded.isEmpty()) {
      uri = HttpURI.from(format("http://hostname/foo/%s/bar", encoded));
      soft.assertThat(uri.getViolations())
          .describedAs("path-middle: %s", encoded)
          .containsExactlyInAnyOrderElementsOf(expectedViolations);
    }
    uri = HttpURI.from(format("http://hostname/bar?foo=%s", encoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param: %s", encoded).isEmpty();
  }

  static Stream<Arguments> fromPathString() {
    return Stream.of(
        arguments("", List.of(), Set.of()),
        arguments("hello", List.of("hello"), Set.of()),
        arguments("he#llo", List.of("he#llo"), Set.of()),
        arguments("he/llo", List.of("he/llo"), Set.of()),
        arguments("hello..~", List.of("hello."), Set.of()),
        arguments("..~hello..~", List.of(".hello."), Set.of()),
        arguments("..~hello..~~", List.of(".hello.~"), Set.of()),
        arguments("..~hello..~~hello", List.of(".hello.~hello"), Set.of()),
        arguments("..~hello...~~hello", List.of(".hello", ".~hello"), Set.of()),
        arguments("..~hello..~.~hello", List.of(".hello.", "~hello"), Set.of()),
        arguments("..~hello..~~.~hello", List.of(".hello.~", "~hello"), Set.of()),
        arguments("..~~hello..~", List.of(".~hello."), Set.of()),
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments("hello..~.world", List.of("hello.", "world"), Set.of()),
        arguments("hello...~world", List.of("hello", ".world"), Set.of()),
        arguments("he..~llo.wo..~rld", List.of("he.llo", "wo.rld"), Set.of()),
        // control-characters cause violations
        arguments(
            "he\u001dllo.wo\u001drld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS, ILLEGAL_PATH_CHARACTERS)),
        arguments(
            "he\u0000llo.wo\u0000rld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS, ILLEGAL_PATH_CHARACTERS)));
  }

  @ParameterizedTest
  @MethodSource
  public void toPathString(
      String encoded,
      List<String> elements,
      Collection<UriCompliance.Violation> expectedViolations) {
    String actualEncoded = Util.toPathString(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded.replace('\u0000', '\u001d'));
    soft.assertThat(encoded.replace('\u0000', '\u001d')).isEqualTo(Util.toPathString(elements));

    HttpURI uri = HttpURI.from(format("http://hostname/%s", actualEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-beginning: %s", actualEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    uri = HttpURI.from(format("http://hostname/foo/%s", actualEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end: %s", actualEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", actualEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end+fragment: %s", actualEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    if (!actualEncoded.isEmpty()) {
      uri = HttpURI.from(format("http://hostname/foo/%s/bar", actualEncoded));
      soft.assertThat(uri.getViolations())
          .describedAs("path-middle: %s", actualEncoded)
          .containsExactlyInAnyOrderElementsOf(expectedViolations);
    }
    uri = HttpURI.from(format("http://hostname/bar?foo=%s", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param: %s", actualEncoded).isEmpty();
    uri = HttpURI.from(format("http://hostname/bar?foo=%s#fragment", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query+fragment: %s", actualEncoded).isEmpty();
  }

  static Stream<Arguments> toPathString() {
    return Stream.of(
        arguments("", List.of(), Set.of()),
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments("he#llo.world", List.of("he#llo", "world"), Set.of()),
        arguments("he/llo.world", List.of("he/llo", "world"), Set.of()),
        // control-characters cause violations
        arguments(
            "he\u001dllo.wo\u001drld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS, ILLEGAL_PATH_CHARACTERS)),
        arguments(
            "he\u0000llo.wo\u0000rld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS, ILLEGAL_PATH_CHARACTERS)));
  }

  @ParameterizedTest
  @MethodSource
  public void toPathStringEscaped(String encoded, List<String> elements) {
    String actualEncoded = Util.toPathStringEscaped(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded);
    soft.assertThat(encoded).isEqualTo(Util.toPathStringEscaped(elements));

    HttpURI uri = HttpURI.from(format("http://hostname/%s", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-beginning: %s", actualEncoded).isEmpty();
    uri = HttpURI.from(format("http://hostname/foo/%s", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-end: %s", actualEncoded).isEmpty();
    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", actualEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end+fragment: %s", actualEncoded)
        .isEmpty();
    if (!actualEncoded.isEmpty()) {
      uri = HttpURI.from(format("http://hostname/foo/%s/bar", actualEncoded));
      soft.assertThat(uri.getViolations()).describedAs("path-middle: %s", actualEncoded).isEmpty();
    }
    uri = HttpURI.from(format("http://hostname/bar?foo=%s", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param: %s", actualEncoded).isEmpty();
    uri = HttpURI.from(format("http://hostname/bar?foo=%s#fragment", actualEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query+fragment: %s", actualEncoded).isEmpty();
  }

  static Stream<Arguments> toPathStringEscaped() {
    return Stream.of(
        arguments("", List.of()),
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he/llo.world", List.of("he/llo", "world")),
        arguments("he..~llo.wo..~rld", List.of("he.llo", "wo.rld")),
        arguments("he..~llo.wo..~rld", List.of("he.llo", "wo.rld")),
        arguments("he..~l#/lo~.~wo..~rld", List.of("he.l#/lo~", "~wo.rld")));
  }
}
