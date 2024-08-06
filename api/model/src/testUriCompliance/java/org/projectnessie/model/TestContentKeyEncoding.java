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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_ENCODING;
import static org.eclipse.jetty.http.UriCompliance.Violation.AMBIGUOUS_PATH_SEPARATOR;
import static org.eclipse.jetty.http.UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.UriCompliance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(SoftAssertionsExtension.class)
public class TestContentKeyEncoding {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void fromPathStringEmpty() {
    List<String> actualElements = Util.fromPathString("");
    soft.assertThat(actualElements).containsExactlyElementsOf(List.of());
    soft.assertThat(actualElements).containsExactlyElementsOf(Util.fromPathString(""));
  }

  @ParameterizedTest
  @MethodSource
  public void fromPathString(
      String encoded,
      List<String> elements,
      Collection<UriCompliance.Violation> expectedViolations) {
    List<String> actualElements = Util.fromPathString(encoded);
    soft.assertThat(actualElements).containsExactlyElementsOf(elements);
    soft.assertThat(actualElements).containsExactlyElementsOf(Util.fromPathString(encoded));

    String uriEncoded = URLEncoder.encode(encoded, UTF_8);

    HttpURI uri = HttpURI.from(format("http://hostname/%s", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-beginning: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[1]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s/bar", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-middle: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param1: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s&bar=moo", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param2: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);
  }

  static Stream<Arguments> fromPathString() {
    return Stream.of(
        arguments("hello", List.of("hello"), Set.of()),
        arguments("he#llo", List.of("he#llo"), Set.of()),
        arguments("he..^llo.world", List.of("he/llo", "world"), Set.of()),
        arguments("hello..*", List.of("hello."), Set.of()),
        arguments("..*hello..*", List.of(".hello."), Set.of()),
        arguments("..*hello..*~", List.of(".hello.~"), Set.of()),
        arguments("..*hello..*~hello", List.of(".hello.~hello"), Set.of()),
        arguments("..*hello...*~hello", List.of(".hello", ".~hello"), Set.of()),
        arguments("..*hello..*.~hello", List.of(".hello.", "~hello"), Set.of()),
        arguments("..*hello..*...*~hello", List.of(".hello.", ".~hello"), Set.of()),
        arguments("..*hello..*~.~hello", List.of(".hello.~", "~hello"), Set.of()),
        arguments("..*~hello..*", List.of(".~hello."), Set.of()),
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments("hello..*.world", List.of("hello.", "world"), Set.of()),
        arguments("hello...*world", List.of("hello", ".world"), Set.of()),
        arguments("he..*llo.wo..*rld", List.of("he.llo", "wo.rld"), Set.of()),
        // path separators cause violations
        arguments("he/llo", List.of("he/llo"), Set.of(AMBIGUOUS_PATH_SEPARATOR)),
        // control-characters cause violations
        arguments(
            "he\u001dllo.wo\u001drld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS)),
        // backslash is a violation
        arguments("he\\llo.world", List.of("he\\llo", "world"), Set.of(SUSPICIOUS_PATH_CHARACTERS)),
        // % alone is ambiguous
        arguments("he%llo.world", List.of("he%llo", "world"), Set.of(AMBIGUOUS_PATH_ENCODING)));
  }

  @Test
  public void toPathStringEmpty() {
    String actualEncoded = Util.toPathString(List.of());
    soft.assertThat(actualEncoded).isEqualTo("");
    soft.assertThat("").isEqualTo(Util.toPathString(List.of()));
  }

  @ParameterizedTest
  @MethodSource
  public void toPathString(
      String encoded,
      List<String> elements,
      Collection<UriCompliance.Violation> expectedViolations) {
    String actualEncoded = Util.toPathString(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded);
    soft.assertThat(encoded).isEqualTo(Util.toPathString(elements));

    String uriEncoded = URLEncoder.encode(actualEncoded, UTF_8);

    HttpURI uri = HttpURI.from(format("http://hostname/%s", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-beginning: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[1]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end+fragment: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s/bar", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-middle: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolations);
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query+fragment: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);
  }

  static Stream<Arguments> toPathString() {
    return Stream.of(
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments("he#llo.world", List.of("he#llo", "world"), Set.of()),
        arguments("he?llo.world", List.of("he?llo", "world"), Set.of()),
        arguments("he&llo.world", List.of("he&llo", "world"), Set.of()),
        // path character causes violations
        arguments("he/llo.world", List.of("he/llo", "world"), Set.of(AMBIGUOUS_PATH_SEPARATOR)),
        // control-characters cause violations
        arguments(
            "he\u001dllo.wo\u001drld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS)));
  }

  @Test
  public void toPathStringEscapedEmpty() {
    String actualEncoded = Util.toPathStringEscaped(List.of());
    soft.assertThat(actualEncoded).isEqualTo("");
    soft.assertThat("").isEqualTo(Util.toPathStringEscaped(List.of()));
  }

  @ParameterizedTest
  @MethodSource
  public void toPathStringEscaped(String encoded, List<String> elements) {
    String actualEncoded = Util.toPathStringEscaped(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded);
    soft.assertThat(encoded).isEqualTo(Util.toPathStringEscaped(elements));

    String uriEncoded = URLEncoder.encode(actualEncoded, UTF_8);

    HttpURI uri = HttpURI.from(format("http://hostname/%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-beginning: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[1]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-end: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-end+fragment: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s/bar", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-middle: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query-param: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/bar?foo=%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("query+fragment: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);
  }

  static Stream<Arguments> toPathStringEscaped() {
    return Stream.of(
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo", List.of("he#llo")),
        arguments("he..-llo", List.of("he\\llo")),
        arguments("he..=llo", List.of("he%llo")),
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he&llo.world", List.of("he&llo", "world")),
        arguments("he?llo.world", List.of("he?llo", "world")),
        arguments("he..^llo.world", List.of("he/llo", "world")),
        arguments("hello..*", List.of("hello.")),
        arguments("..*hello..*", List.of(".hello.")),
        arguments("..*hello..*~", List.of(".hello.~")),
        arguments("..*hello..*~hello", List.of(".hello.~hello")),
        arguments("..*hello...*~hello", List.of(".hello", ".~hello")),
        arguments("..*hello..*.~hello", List.of(".hello.", "~hello")),
        arguments("..*hello..*...*~hello", List.of(".hello.", ".~hello")),
        arguments("..*hello..*~.~hello", List.of(".hello.~", "~hello")),
        arguments("..*~hello..*", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments("hello..*.world", List.of("hello.", "world")),
        arguments("hello...*world", List.of("hello", ".world")),
        arguments("he..*llo.wo..*rld", List.of("he.llo", "wo.rld")),
        arguments("he..^llo.world", List.of("he/llo", "world")),
        arguments("he..*llo.wo..*rld", List.of("he.llo", "wo.rld")),
        arguments("he..*llo.wo..*rld", List.of("he.llo", "wo.rld")),
        arguments("he..*l#..^lo~.~wo..*rld", List.of("he.l#/lo~", "~wo.rld")));
  }
}
