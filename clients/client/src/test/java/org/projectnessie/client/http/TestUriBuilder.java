/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.client.http;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.junit.jupiter.api.Test;

class TestUriBuilder {

  @Test
  void simple() {
    assertEquals(
        "http://localhost/foo/bar/",
        new UriBuilder(URI.create("http://localhost/foo/bar/")).build().toString());
  }

  @Test
  void parameterValidation() {
    assertAll(
        () -> assertThrows(NullPointerException.class, () -> new UriBuilder(null)),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> new UriBuilder(URI.create("http://base/")).path(null)),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> new UriBuilder(URI.create("http://base/")).resolveTemplate(null, "value")),
        () ->
            assertThrows(
                NullPointerException.class,
                () -> new UriBuilder(URI.create("http://base/")).resolveTemplate("name", null)));
  }

  @Test
  void addMissingSlash() {
    assertEquals(
        "http://localhost/", new UriBuilder(URI.create("http://localhost")).build().toString());
    assertEquals(
        "http://localhost/foo/bar",
        new UriBuilder(URI.create("http://localhost")).path("foo").path("bar").build().toString());
  }

  @Test
  void pathTemplates() {
    UriBuilder builder = new UriBuilder(URI.create("http://localhost/foo/bar/"));

    builder = builder.path("{my-var}").resolveTemplate("my-var", "baz");
    assertEquals("http://localhost/foo/bar/baz", builder.build().toString());

    builder = builder.path("something/{in}/here").resolveTemplate("in", "out");
    assertEquals("http://localhost/foo/bar/baz/something/out/here", builder.build().toString());

    builder = builder.resolveTemplate("no", "boo");
    UriBuilder bulder1 = builder;
    assertEquals(
        String.format(
            "Cannot build uri. Not all template keys (%s) were used in uri %s",
            "{no}", "{my-var}/something/{in}/here"),
        assertThrows(HttpClientException.class, bulder1::build).getMessage());
  }

  @Test
  void pathEncoding() {
    UriBuilder builder = new UriBuilder(URI.create("http://localhost/foo/bar/"));

    builder = builder.path("some spaces in here");
    assertEquals("http://localhost/foo/bar/some%20spaces%20in%20here", builder.build().toString());
  }

  @Test
  void queryParameters() {
    UriBuilder builder = new UriBuilder(URI.create("http://localhost/foo/bar/"));

    builder = builder.queryParam("a", "b");
    assertEquals("http://localhost/foo/bar/?a=b", builder.build().toString());

    builder = builder.queryParam("c", "d");
    assertEquals("http://localhost/foo/bar/?a=b&c=d", builder.build().toString());

    builder = builder.queryParam("e", "f&? /");
    assertEquals("http://localhost/foo/bar/?a=b&c=d&e=f%26%3F%20%2F", builder.build().toString());

    builder = builder.queryParam("c", "d-more");
    assertEquals(
        "http://localhost/foo/bar/?a=b&c=d&e=f%26%3F%20%2F&c=d-more", builder.build().toString());
  }
}
