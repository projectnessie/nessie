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
package org.projectnessie.client.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;

class TestOAuth2ResponseFilter {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ParameterizedTest
  @MethodSource
  void filter(
      Status status,
      boolean json,
      String body,
      HttpClientResponseException expected,
      Class<?> suppressedType) {
    OAuth2ResponseFilter filter = new OAuth2ResponseFilter(OBJECT_MAPPER);
    ResponseContext context =
        new ResponseContext() {
          @Override
          public Status getStatus() {
            return status;
          }

          @Override
          public InputStream getInputStream() {
            if (body == null) {
              return null;
            }
            return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
          }

          @Override
          public String getContentType() {
            return json ? "application/json" : "text/plain";
          }

          @Override
          public URI getRequestedUri() {
            return URI.create("https://auth.com/token");
          }
        };
    Throwable error = catchThrowable(() -> filter.filter(context));
    if (expected == null) {
      assertThat(error).isNull();
    } else {
      assertThat(error)
          .isExactlyInstanceOf(expected.getClass())
          .hasMessage(expected.getMessage())
          .usingRecursiveComparison()
          .isEqualTo(expected);
      if (suppressedType != null) {
        assertThat(error.getSuppressed()).singleElement().isInstanceOf(suppressedType);
      } else {
        assertThat(error).hasNoSuppressedExceptions();
      }
    }
  }

  public static Stream<Arguments> filter() {
    return Stream.of(
        Arguments.of(Status.OK, true, "body", null, null),
        Arguments.of(Status.FOUND, true, "body", null, null),
        Arguments.of(
            Status.MOVED_PERMANENTLY,
            false,
            null,
            new HttpClientResponseException(
                "Request to https://auth.com/token failed with HTTP/301 (response body was empty)",
                Status.MOVED_PERMANENTLY),
            null),
        Arguments.of(
            Status.BAD_REQUEST,
            true,
            "{\"error\":\"42\",\"error_description\":\"Try Again\"}",
            new OAuth2Exception(
                "Request to https://auth.com/token failed with HTTP/400 and error code \"42\": Try Again",
                Status.BAD_REQUEST,
                ImmutableErrorResponse.builder()
                    .errorCode("42")
                    .errorDescription("Try Again")
                    .build()),
            null),
        Arguments.of(
            Status.BAD_REQUEST,
            true,
            "{\"error\":\"42\"}",
            new OAuth2Exception(
                "Request to https://auth.com/token failed with HTTP/400 and error code \"42\"",
                Status.BAD_REQUEST,
                ImmutableErrorResponse.builder().errorCode("42").build()),
            null),
        Arguments.of(
            Status.BAD_REQUEST,
            true,
            "{\"strange_json\":\"foo\"}",
            new HttpClientResponseException(
                "Request to https://auth.com/token failed with HTTP/400 and unparseable response body: {\"strange_json\":\"foo\"}",
                Status.BAD_REQUEST),
            JsonMappingException.class),
        Arguments.of(
            Status.NOT_FOUND,
            false,
            "Try Again",
            new HttpClientResponseException(
                "Request to https://auth.com/token failed with HTTP/404 and unparseable response body: Try Again",
                Status.NOT_FOUND),
            null));
  }
}
