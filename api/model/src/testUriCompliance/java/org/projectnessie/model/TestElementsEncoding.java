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

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.List;
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
            elements.get(elements.size() - 1),
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
        arguments("hello", List.of("hello"), Set.of()),
        //
        arguments("foo.bar.baz", List.of("foo", "bar", "baz"), Set.of()),
        // Escaping can start at the 1st element...
        arguments(".foo.bar.baz", List.of("foo", "bar", "baz"), Set.of()),
        // .. at the 2nd element...
        arguments("foo..bar.baz", List.of("foo", "bar", "baz"), Set.of()),
        // .. or any other element.
        arguments("foo.bar..baz", List.of("foo", "bar", "baz"), Set.of()),
        //
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        arguments("foo.._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo..{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo..}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo..[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        //
        arguments(".he.[llo", List.of("he%llo"), Set.of()),
        arguments(".he.{llo.world", List.of("he/llo", "world"), Set.of()),
        arguments(".hello._", List.of("hello."), Set.of()),
        arguments(".._hello._", List.of(".hello."), Set.of()),
        arguments(".._hello._~", List.of(".hello.~"), Set.of()),
        arguments(".._hello._~hello", List.of(".hello.~hello"), Set.of()),
        arguments(".._hello..._~hello", List.of(".hello", ".~hello"), Set.of()),
        arguments(".._hello._.~hello", List.of(".hello.", "~hello"), Set.of()),
        arguments(".._hello._..._~hello", List.of(".hello.", ".~hello"), Set.of()),
        arguments(".._hello._~.~hello", List.of(".hello.~", "~hello"), Set.of()),
        arguments(".._~hello._", List.of(".~hello."), Set.of()),
        arguments("hello.world", List.of("hello", "world"), Set.of()),
        arguments(".hello._.world", List.of("hello.", "world"), Set.of()),
        arguments("hello..._world", List.of("hello", ".world"), Set.of()),
        arguments(".he._llo.wo._rld", List.of("he.llo", "wo.rld"), Set.of()),
        // valid for canonical representation, path separators cause violations
        arguments("he/llo", List.of("he/llo"), Set.of(AMBIGUOUS_PATH_SEPARATOR)),
        // valid for canonical representation, backslash is a violation
        arguments("he\\llo.world", List.of("he\\llo", "world"), Set.of(SUSPICIOUS_PATH_CHARACTERS)),
        // valid for canonical representation, % is ambiguous
        arguments("he%llo.world", List.of("he%llo", "world"), Set.of(AMBIGUOUS_PATH_ENCODING)),
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
        //
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        //
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo", List.of("he#llo")),
        arguments(".he.}llo", List.of("he\\llo")),
        arguments(".he.}llo", List.of("he\\llo")),
        arguments(".he.[llo", List.of("he%llo")),
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he&llo.world", List.of("he&llo", "world")),
        arguments("he?llo.world", List.of("he?llo", "world")),
        arguments(".he.{llo..world", List.of("he/llo", "world")),
        arguments(".hello._", List.of("hello.")),
        arguments(".._hello._", List.of(".hello.")),
        arguments(".._hello._~", List.of(".hello.~")),
        arguments(".._hello._~hello", List.of(".hello.~hello")),
        arguments(".._hello..._~hello", List.of(".hello", ".~hello")),
        arguments(".._hello._..~hello", List.of(".hello.", "~hello")),
        arguments(".._hello._..._~hello", List.of(".hello.", ".~hello")),
        arguments(".._hello._~..~hello", List.of(".hello.~", "~hello")),
        arguments(".._~hello._", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments(".hello._..world", List.of("hello.", "world")),
        arguments("hello..._world", List.of("hello", ".world")),
        arguments(".he._llo..wo._rld", List.of("he.llo", "wo.rld")),
        arguments(".he.{llo..world", List.of("he/llo", "world")),
        arguments(".he._llo..wo._rld", List.of("he.llo", "wo.rld")),
        arguments(".he._l#.{lo~..~wo._rld", List.of("he.l#/lo~", "~wo.rld")));
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
        arguments("foo._ar.baz", List.of("foo", "_ar", "baz"), Set.of()),
        arguments("foo.{ar.baz", List.of("foo", "{ar", "baz"), Set.of()),
        arguments("foo.}ar.baz", List.of("foo", "}ar", "baz"), Set.of()),
        arguments("foo.[ar.baz", List.of("foo", "[ar", "baz"), Set.of()),
        //
        arguments("hello.world", List.of("hello", "world")),
        arguments("he#llo", List.of("he#llo")),
        arguments("he/llo", List.of("he/llo")),
        arguments("he\\llo", List.of("he\\llo")),
        arguments("he%llo", List.of("he%llo")),
        arguments("he#llo.world", List.of("he#llo", "world")),
        arguments("he&llo.world", List.of("he&llo", "world")),
        arguments("he?llo.world", List.of("he?llo", "world")),
        arguments("he/llo.world", List.of("he/llo", "world")),
        arguments(".hello._", List.of("hello.")),
        arguments(".._hello._", List.of(".hello.")),
        arguments(".._hello._~", List.of(".hello.~")),
        arguments(".._hello._~hello", List.of(".hello.~hello")),
        arguments(".._hello..._~hello", List.of(".hello", ".~hello")),
        arguments(".._hello._..~hello", List.of(".hello.", "~hello")),
        arguments(".._hello._..._~hello", List.of(".hello.", ".~hello")),
        arguments(".._hello._~..~hello", List.of(".hello.~", "~hello")),
        arguments(".._~hello._", List.of(".~hello.")),
        arguments("hello.world", List.of("hello", "world")),
        arguments(".hello._..world", List.of("hello.", "world")),
        arguments("hello..._world", List.of("hello", ".world")),
        arguments(".he._llo..wo._rld", List.of("he.llo", "wo.rld")),
        arguments("he/llo.world", List.of("he/llo", "world")),
        arguments(".he._llo..wo._rld", List.of("he.llo", "wo.rld")),
        arguments(".he._l#/lo~..~wo._rld", List.of("he.l#/lo~", "~wo.rld")));
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
        arguments(List.of("abc.", "{def"), ".abc._..{def", ".abc._..{def", "abc\u001d.{def"),
        arguments(List.of("abc.", ".def"), ".abc._..._def", ".abc._..._def", "abc\u001d.\u001ddef"),
        arguments(List.of("abc.", "}def"), ".abc._..}def", ".abc._..}def", "abc\u001d.}def"),
        arguments(List.of("abc.", "[def"), ".abc._..[def", ".abc._..[def", "abc\u001d.[def"),
        arguments(List.of("abc.", "/def"), ".abc._...{def", ".abc._../def", "abc\u001d./def"),
        arguments(List.of("abc.", "%def"), ".abc._...[def", ".abc._..%def", "abc\u001d.%def"),
        arguments(List.of("abc.", "\\def"), ".abc._...}def", ".abc._..\\def", "abc\u001d.\\def"),
        arguments(List.of("/%", "#&&"), "..{.[..#&&", "/%.#&&", "/%.#&&"),
        arguments(List.of("/%国", "国.国"), "..{.[国..国._国", "/%国..国._国", "/%国.国\u001d国")
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
}
