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
import static org.eclipse.jetty.http.UriCompliance.Violation.FRAGMENT;
import static org.eclipse.jetty.http.UriCompliance.Violation.SUSPICIOUS_PATH_CHARACTERS;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
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
public class TestElementsEncoding {
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

    soft.assertThat(Namespace.fromPathString(encoded))
        .extracting(Namespace::getElements, Namespace::getElementCount)
        .containsExactly(elements, elements.size());
    soft.assertThat(ContentKey.fromPathString(encoded))
        .extracting(
            ContentKey::getElements,
            ContentKey::getName,
            ContentKey::getNamespace,
            ContentKey::getElementCount)
        .containsExactly(
            elements,
            elements.getLast(),
            Namespace.of(elements.subList(0, elements.size() - 1)),
            elements.size());
    soft.assertThat(Namespace.fromPathString(encoded))
        .extracting(Namespace::toContentKey)
        .isEqualTo(ContentKey.fromPathString(encoded));
    if (elements.size() > 1) {
      soft.assertThat(Namespace.fromPathString(encoded))
          .extracting(Namespace::getParent)
          .isEqualTo(Namespace.of(elements.subList(0, elements.size() - 1)));
      soft.assertThat(ContentKey.fromPathString(encoded))
          .extracting(ContentKey::getParent)
          .isEqualTo(ContentKey.of(elements.subList(0, elements.size() - 1)));
    }

    String uriEncoded = URLEncoder.encode(encoded, UTF_8);
    soft.assertThat(URLDecoder.decode(uriEncoded, UTF_8)).isEqualTo(encoded);

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
        // 1
        arguments("hello", List.of("hello"), Set.of()),
        arguments("foo.bar.baz", List.of("foo", "bar", "baz"), Set.of()),
        arguments(".foo.bar.baz", List.of("foo", "bar", "baz"), Set.of()),
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        // 5
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        arguments(".foo.**ar.baz", List.of("foo", "*ar", "baz"), Set.of()),
        arguments(".foo.*.ar.baz", List.of("foo", ".ar", "baz"), Set.of()),
        // 10
        arguments(".foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments(".foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        arguments(".he*[llo", List.of("he%llo"), Set.of()),
        arguments(".he*{llo.world", List.of("he/llo", "world"), Set.of()),
        arguments(".hello*.", List.of("hello."), Set.of()),
        // 15
        arguments(".*.hello*.", List.of(".hello."), Set.of()),
        arguments(".*.hello*.~", List.of(".hello.~"), Set.of()),
        arguments(".*.hello*.~hello", List.of(".hello.~hello"), Set.of()),
        arguments(".*.hello.*.~hello", List.of(".hello", ".~hello"), Set.of()),
        arguments(".*.hello*..~hello", List.of(".hello.", "~hello"), Set.of()),
        // 20
        arguments(".*.hello*..*.~hello", List.of(".hello.", ".~hello"), Set.of()),
        arguments(".*.hello*.~.~hello", List.of(".hello.~", "~hello"), Set.of()),
        arguments(".*.~hello*.", List.of(".~hello."), Set.of()),
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments(".hello*..world", List.of("hello.", "world"), Set.of()),
        // 25
        arguments(".hello.*.world", List.of("hello", ".world"), Set.of()),
        arguments(".he*.llo.wo*.rld", List.of("he.llo", "wo.rld"), Set.of()),
        // valid for canonical representation, path separators cause violations
        arguments("he/llo", List.of("he/llo"), Set.of(AMBIGUOUS_PATH_SEPARATOR)),
        // valid for canonical representation, backslash is a violation
        arguments("he\\llo.world", List.of("he\\llo", "world"), Set.of(SUSPICIOUS_PATH_CHARACTERS)),
        // valid for canonical representation, % is ambiguous
        arguments("he%llo.world", List.of("he%llo", "world"), Set.of(AMBIGUOUS_PATH_ENCODING)),
        // 30
        // control-characters cause violations
        arguments(
            "he\u001dllo.wo\u001drld",
            List.of("he.llo", "wo.rld"),
            Set.of(SUSPICIOUS_PATH_CHARACTERS)));
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
    soft.assertThat(URLDecoder.decode(uriEncoded, UTF_8)).isEqualTo(actualEncoded);

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

    var expectedViolationsWithFragment = new ArrayList<>(expectedViolations);
    expectedViolationsWithFragment.add(FRAGMENT);

    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end+fragment: %s", uriEncoded)
        .containsExactlyInAnyOrderElementsOf(expectedViolationsWithFragment);
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
    soft.assertThat(uri.getViolations())
        .describedAs("query+fragment: %s", uriEncoded)
        .containsExactly(FRAGMENT);
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
    soft.assertThat(Util.fromPathString(encoded)).containsExactlyElementsOf(elements);

    String uriEncoded = URLEncoder.encode(actualEncoded, UTF_8);
    soft.assertThat(URLDecoder.decode(uriEncoded, UTF_8)).isEqualTo(actualEncoded);

    HttpURI uri = HttpURI.from(format("http://hostname/%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-beginning: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[1]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s", uriEncoded));
    soft.assertThat(uri.getViolations()).describedAs("path-end: %s", uriEncoded).isEmpty();
    soft.assertThat(uri.getPath().split("/")[2]).describedAs(uri.getPath()).isEqualTo(uriEncoded);

    uri = HttpURI.from(format("http://hostname/foo/%s#fragment", uriEncoded));
    soft.assertThat(uri.getViolations())
        .describedAs("path-end+fragment: %s", uriEncoded)
        .containsExactly(FRAGMENT);
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
    soft.assertThat(uri.getViolations())
        .describedAs("query+fragment: %s", uriEncoded)
        .containsExactly(FRAGMENT);
    soft.assertThat(uri.getQuery().split("&")[0].split("=")[1])
        .describedAs(uri.getQuery())
        .isEqualTo(uriEncoded);
  }

  static Stream<Arguments> toPathStringEscaped() {
    return Stream.of(
        // 1
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        // 5
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo", List.of("he#llo")),
        arguments(".he*{llo", List.of("he/llo")),
        arguments(".he*}llo", List.of("he\\llo")),
        arguments(".he*[llo", List.of("he%llo")),
        // 10
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he&llo.world", List.of("he&llo", "world")),
        arguments("he?llo.world", List.of("he?llo", "world")),
        arguments(".he*{llo.world", List.of("he/llo", "world")),
        arguments(".hello*.", List.of("hello.")),
        // 15
        arguments(".*.hello*.", List.of(".hello.")),
        arguments(".*.hello*.~", List.of(".hello.~")),
        arguments(".*.hello*.~hello", List.of(".hello.~hello")),
        arguments(".*.hello.*.~hello", List.of(".hello", ".~hello")),
        arguments(".*.hello*..~hello", List.of(".hello.", "~hello")),
        // 20
        arguments(".*.hello*..*.~hello", List.of(".hello.", ".~hello")),
        arguments(".*.hello*.~.~hello", List.of(".hello.~", "~hello")),
        arguments(".*.~hello*.", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments(".hello*..world", List.of("hello.", "world")),
        // 25
        arguments(".hello.*.world", List.of("hello", ".world")),
        arguments(".he*.llo.wo*.rld", List.of("he.llo", "wo.rld")),
        arguments(".he*{llo.world", List.of("he/llo", "world")),
        arguments(".he*.llo.wo*.rld", List.of("he.llo", "wo.rld")),
        arguments(".he*.l#*{lo~.~wo*.rld", List.of("he.l#/lo~", "~wo.rld"))
        // 30
        );
  }

  @ParameterizedTest
  @MethodSource
  public void toCanonicalString(String encoded, List<String> elements) {
    String actualEncoded = Util.toCanonicalString(elements);
    soft.assertThat(actualEncoded).isEqualTo(encoded);
    soft.assertThat(encoded).isEqualTo(Util.toCanonicalString(elements));
    soft.assertThat(Util.fromPathString(encoded)).containsExactlyElementsOf(elements);
  }

  static Stream<Arguments> toCanonicalString() {
    return Stream.of(
        // 1
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        // 5
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo", List.of("he#llo")),
        arguments("he/llo", List.of("he/llo")),
        arguments("he\\llo", List.of("he\\llo")),
        arguments("he%llo", List.of("he%llo")),
        // 10
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he&llo.world", List.of("he&llo", "world")),
        arguments("he?llo.world", List.of("he?llo", "world")),
        arguments("he/llo.world", List.of("he/llo", "world")),
        arguments(".hello*.", List.of("hello.")),
        // 15
        arguments(".*.hello*.", List.of(".hello.")),
        arguments(".*.hello*.~", List.of(".hello.~")),
        arguments(".*.hello*.~hello", List.of(".hello.~hello")),
        arguments(".*.hello.*.~hello", List.of(".hello", ".~hello")),
        arguments(".*.hello*..~hello", List.of(".hello.", "~hello")),
        // 20
        arguments(".*.hello*..*.~hello", List.of(".hello.", ".~hello")),
        arguments(".*.hello*.~.~hello", List.of(".hello.~", "~hello")),
        arguments(".*.~hello*.", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments(".hello*..world", List.of("hello.", "world")),
        // 25
        arguments(".hello.*.world", List.of("hello", ".world")),
        arguments(".he*.llo.wo*.rld", List.of("he.llo", "wo.rld")),
        arguments("he/llo.world", List.of("he/llo", "world")),
        arguments(".he*.llo.wo*.rld", List.of("he.llo", "wo.rld")),
        arguments(".he*.l#/lo~.~wo*.rld", List.of("he.l#/lo~", "~wo.rld")));
  }

  @ParameterizedTest
  @MethodSource
  void roundTrips(
      List<String> elements,
      String expectedEscaped,
      String expectedCanonical,
      String expectedLegacy) {
    String escaped = Util.toPathStringEscaped(elements);
    String canonical = Util.toCanonicalString(elements);
    String legacy = Util.toPathString(elements);
    List<String> actualFromEscaped = Util.fromPathString(escaped);
    List<String> actualFromCanonical = Util.fromPathString(canonical);
    List<String> actualFromLegacy = Util.fromPathString(legacy);
    String uriEncodedEscaped = URLEncoder.encode(escaped, UTF_8);
    String uriEncodedLegacy = URLEncoder.encode(legacy, UTF_8);

    HttpURI uri = HttpURI.from(format("http://hostname/foo/%s", uriEncodedEscaped));
    soft.assertThat(uri.getViolations())
        .describedAs("escaped: %s -  url: %s", escaped, uriEncodedEscaped)
        .isEmpty();

    soft.assertThat(escaped).isEqualTo(expectedEscaped);
    soft.assertThat(canonical).isEqualTo(expectedCanonical);
    soft.assertThat(legacy).isEqualTo(expectedLegacy);
    soft.assertThat(actualFromEscaped).isEqualTo(elements);
    soft.assertThat(actualFromCanonical).isEqualTo(elements);
    soft.assertThat(actualFromLegacy).isEqualTo(elements);
    soft.assertThat(URLDecoder.decode(uriEncodedEscaped, UTF_8)).isEqualTo(escaped);
    soft.assertThat(URLDecoder.decode(uriEncodedLegacy, UTF_8)).isEqualTo(legacy);
  }

  static Stream<Arguments> roundTrips() {
    return Stream.of(
        // 1
        arguments(List.of("abc.", "*def"), ".abc*..**def", ".abc*..**def", "abc\u001d.*def"),
        arguments(List.of("abc*", ".def"), ".abc**.*.def", ".abc**.*.def", "abc*.\u001ddef"),
        arguments(List.of("abc*", "*def"), "abc*.*def", "abc*.*def", "abc*.*def"),
        arguments(List.of("abc.", "{def"), ".abc*..{def", ".abc*..{def", "abc\u001d.{def"),
        // 5
        arguments(List.of("abc.", ".def"), ".abc*..*.def", ".abc*..*.def", "abc\u001d.\u001ddef"),
        arguments(List.of("abc.", "}def"), ".abc*..}def", ".abc*..}def", "abc\u001d.}def"),
        arguments(List.of("abc.", "[def"), ".abc*..[def", ".abc*..[def", "abc\u001d.[def"),
        arguments(List.of("abc.", "/def"), ".abc*..*{def", ".abc*../def", "abc\u001d./def"),
        arguments(List.of("abc.", "%def"), ".abc*..*[def", ".abc*..%def", "abc\u001d.%def"),
        // 10
        arguments(List.of("abc.", "\\def"), ".abc*..*}def", ".abc*..\\def", "abc\u001d.\\def"),
        arguments(List.of("/%", "#&&"), ".*{*[.#&&", "/%.#&&", "/%.#&&"),
        arguments(List.of("/%国", "国.国"), ".*{*[国.国*.国", "./%国.国*.国", "/%国.国\u001d国")
        //
        );
  }

  @ParameterizedTest
  @MethodSource
  void legacyZero(String legacy, List<String> elements) {
    soft.assertThat(Util.fromPathString(legacy)).containsExactlyElementsOf(elements);
    soft.assertThat(Util.fromPathString(legacy.replace('\u0000', '\u001d')))
        .containsExactlyElementsOf(elements);
  }

  static Stream<Arguments> legacyZero() {
    return Stream.of(
        arguments("a\u0000b", List.of("a.b")),
        arguments("\u0000b", List.of(".b")),
        arguments("a\u0000", List.of("a.")),
        arguments("a\u0000b.x", List.of("a.b", "x")),
        arguments("\u0000b.x", List.of(".b", "x")),
        arguments("a\u0000.x", List.of("a.", "x")),
        arguments("x.a\u0000b", List.of("x", "a.b")),
        arguments("x.\u0000b", List.of("x", ".b")),
        arguments("x.a\u0000", List.of("x", "a.")));
  }

  @ParameterizedTest
  @MethodSource
  void invalidEncoded(String encoded, String message) {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> ContentKey.fromPathString(encoded))
        .withMessageStartingWith("Content key")
        .withMessageEndingWith(message);
    soft.assertThatIllegalArgumentException()
        .isThrownBy(() -> Namespace.fromPathString(encoded))
        .withMessageStartingWith("Namespace")
        .withMessageEndingWith(message);
  }

  static Stream<Arguments> invalidEncoded() {
    return IntStream.range(1, 0x1f)
        .filter(i -> i != 0x1d)
        .mapToObj(i -> arguments("" + (char) i, "must not contain characters less than 0x20"));
  }

  @ParameterizedTest
  @MethodSource
  void randomRoundTrips(List<String> elements) {
    String escaped = Util.toPathStringEscaped(elements);
    String canonical = Util.toCanonicalString(elements);

    String uriEncodedEscaped = URLEncoder.encode(escaped, UTF_8);
    HttpURI uri = HttpURI.from(format("http://hostname/foo/%s", uriEncodedEscaped));
    soft.assertThat(uri.getViolations())
        .describedAs("escaped: %s -  url: %s", escaped, uriEncodedEscaped)
        .isEmpty();

    List<String> elementsFromEscaped = Util.fromPathString(escaped);
    List<String> elementsFromCanonical = Util.fromPathString(canonical);

    soft.assertThat(elementsFromEscaped).containsExactlyElementsOf(elements);
    soft.assertThat(elementsFromCanonical).containsExactlyElementsOf(elements);
  }

  @SuppressWarnings("AvoidEscapedUnicodeCharacters")
  static List<List<String>> randomRoundTrips() {
    char[] chars = {
      'a', 'b', 'c', 'ä', 'é', '\uD83D', '\uDE04', '《', '*', '.', '{', '}', '[', '~', '/', '\\', '%'
    };

    List<List<String>> r = new ArrayList<>(500) {};
    Random rand = new Random(5733746443528016288L); // From some random
    for (int i = 0; i < 500; i++) {
      List<String> elements = new ArrayList<>();
      for (int e = 0; e < 3; e++) {
        StringBuilder sb = new StringBuilder();
        for (int c = 0; c < 6; c++) {
          sb.append(chars[rand.nextInt(chars.length)]);
        }
        elements.add(sb.toString());
      }
      r.add(elements);
    }
    return r;
  }

  @ParameterizedTest
  @MethodSource
  void allCharacters(String element) {
    List<String> elements = List.of(element);
    String escaped = Util.toPathStringEscaped(elements);
    String canonical = Util.toCanonicalString(elements);

    String uriEncodedEscaped = URLEncoder.encode(escaped, UTF_8);
    HttpURI uri = HttpURI.from(format("http://hostname/foo/%s", uriEncodedEscaped));
    soft.assertThat(uri.getViolations())
        .describedAs("escaped: %s -  url: %s", escaped, uriEncodedEscaped)
        .isEmpty();

    List<String> elementsFromEscaped = Util.fromPathString(escaped);
    List<String> elementsFromCanonical = Util.fromPathString(canonical);

    soft.assertThat(elementsFromEscaped).containsExactlyElementsOf(elements);
    soft.assertThat(elementsFromCanonical).containsExactlyElementsOf(elements);
  }

  static Stream<String> allCharacters() {
    int split = 32;
    return IntStream.rangeClosed(32 / split, 0xffff / split)
        .mapToObj(
            i -> {
              char c = (char) (i * split);
              StringBuilder sb = new StringBuilder(split);
              for (int i1 = 0; i1 < split; i1++) {
                if (c != 0x7f) {
                  sb.append(c);
                }
                c++;
              }
              return sb.toString();
            });
  }

  @Test
  public void allCharsInQuery() {
    StringBuilder sb = new StringBuilder(65536);
    for (int i = 0; i <= 65535; i++) {
      sb.append((char) i);
    }
    HttpURI uri = HttpURI.from(format("http://hostname/foo?%s", sb));
    soft.assertThat(uri.getViolations()).containsExactly(FRAGMENT);
  }
}
