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
package org.projectnessie.client.http.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.projectnessie.client.http.impl.HttpUtils.applyHeaders;

import java.net.URLConnection;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class TestHttpHeaders {
  @Test
  public void multipleValues() {
    HttpHeaders headers = new HttpHeaders();
    headers.put("Abc", "def");
    headers.put("abc", "ghi");
    headers.put("aBc", "jkl");
    assertThat(headers.asMap())
        .containsExactly(entry("Abc", new LinkedHashSet<>(Arrays.asList("def", "ghi", "jkl"))));
    assertThat(headers.getValues("abc")).containsExactly("def", "ghi", "jkl");
  }

  @Test
  public void notPresent() {
    HttpHeaders headers = new HttpHeaders();
    assertThat(headers.getValues("notThere")).isEmpty();
  }

  @Test
  public void applyToUrlConnection() {

    HttpHeaders headers = new HttpHeaders();
    headers.put("Abc", "def");
    headers.put("abc", "ghi");
    headers.put("aBc", "jkl");
    headers.put("Foo-Bar", "Baz");

    URLConnection urlConnectionMock = mock(URLConnection.class);
    ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);

    applyHeaders(headers, urlConnectionMock);

    verify(urlConnectionMock, times(4))
        .addRequestProperty(nameCaptor.capture(), valueCaptor.capture());

    assertThat(nameCaptor.getAllValues()).containsExactly("Abc", "Abc", "Abc", "Foo-Bar");
    assertThat(valueCaptor.getAllValues()).containsExactly("def", "ghi", "jkl", "Baz");
  }

  @ParameterizedTest
  @MethodSource
  void testGetFirstValue(Consumer<HttpHeaders> consumer, String expectedValue) {
    HttpHeaders headers = new HttpHeaders();
    consumer.accept(headers);
    Optional<String> firstValue = headers.getFirstValue("headername");
    if (expectedValue != null) {
      assertThat(firstValue).contains(expectedValue);
    } else {
      assertThat(firstValue).isEmpty();
    }
  }

  static Stream<Arguments> testGetFirstValue() {
    return Stream.of(
        Arguments.of(
            (Consumer<HttpHeaders>)
                headers -> {
                  headers.put("Foo", "value1");
                  headers.put("Bar", "value2");
                },
            null),
        Arguments.of(
            (Consumer<HttpHeaders>)
                headers -> {
                  headers.put("HeaderName", "value1");
                  headers.put("Foo", "value2");
                  headers.put("Bar", "value3");
                },
            "value1"),
        Arguments.of(
            (Consumer<HttpHeaders>)
                headers -> {
                  headers.put("HeaderName", "value1");
                  headers.put("HeaderName", "value2");
                  headers.put("HeaderName", "value3");
                },
            "value1"));
  }
}
