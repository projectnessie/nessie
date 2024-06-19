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
package org.projectnessie.client.http;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import org.junit.jupiter.api.Test;

class TestHttpClientResponseException {

  @Test
  void testGetMessage() {
    HttpClientResponseException exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, null);
    assertThat(exception.getMessage())
        .isEqualTo(
            "Server replied to https://example.com/api with HTTP status code 400 (response body was empty)");
    exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"),
            Status.BAD_REQUEST,
            "<html><body>Try Again</body></html>");
    assertThat(exception.getMessage())
        .isEqualTo(
            "Server replied to https://example.com/api with HTTP status code 400 "
                + "and response body:\n<html><body>Try Again</body></html>");
  }

  @Test
  void testGetStatus() {
    HttpClientResponseException exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, null);
    assertThat(exception.getStatus()).isEqualTo(Status.BAD_REQUEST);
  }

  @Test
  void testGetUri() {
    HttpClientResponseException exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, null);
    assertThat(exception.getUri()).isEqualTo(URI.create("https://example.com/api"));
  }

  @Test
  void testGetBody() {
    HttpClientResponseException exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, "body");
    assertThat(exception.getBody()).contains("body");
    exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, "");
    assertThat(exception.getBody()).isEmpty();
    exception =
        new HttpClientResponseException(
            URI.create("https://example.com/api"), Status.BAD_REQUEST, null);
    assertThat(exception.getBody()).isEmpty();
  }
}
