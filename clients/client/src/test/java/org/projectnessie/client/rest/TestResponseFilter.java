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
package org.projectnessie.client.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieNotFoundException;
import software.amazon.awssdk.utils.StringInputStream;

public class TestResponseFilter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ParameterizedTest
  @MethodSource("provider")
  void testResponseFilter(Status responseCode, Class<? extends Exception> clazz) {
    final NessieError error =
        new NessieError(responseCode.getCode(), responseCode.getReason(), "xxx", null);
    try {
      ResponseCheckFilter.checkResponse(new TestResponseContext(responseCode, error), MAPPER);
    } catch (Exception e) {
      assertThat(e).isInstanceOf(clazz);
      if (e instanceof NessieServiceException) {
        assertThat(((NessieServiceException) e).getError()).isEqualTo(error);
      }
      if (e instanceof BaseNessieClientServerException) {
        assertThat(((BaseNessieClientServerException) e).getStatus()).isEqualTo(error.getStatus());
        assertThat(((BaseNessieClientServerException) e).getServerStackTrace())
            .isEqualTo(error.getServerStackTrace());
      }
    }
  }

  @Test
  void testBadReturn() {
    final NessieError error = new NessieError("unknown", 415, "xxx", null);
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.UNSUPPORTED_MEDIA_TYPE, error), MAPPER))
        .isInstanceOf(NessieServiceException.class)
        .hasMessage("xxx (HTTP/415): unknown");
  }

  @Test
  void testBadReturnNoError() throws IOException {
    try {
      ResponseCheckFilter.checkResponse(
          new ResponseContext() {
            @Override
            public Status getResponseCode() {
              return Status.UNAUTHORIZED;
            }

            @Override
            public InputStream getInputStream() {
              Assertions.fail();
              return null;
            }

            @Override
            public InputStream getErrorStream() {
              return new StringInputStream("this will fail");
            }
          },
          MAPPER);
    } catch (NessieServiceException e) {
      assertThat(Status.UNAUTHORIZED.getCode()).isEqualTo(e.getError().getStatus());
      assertThat(e.getError().getClientProcessingException()).isInstanceOf(IOException.class);
      assertThat(e.getError().getServerStackTrace()).isNull();
    }
  }

  @Test
  void testBadReturnBadError() {
    NessieError defaultError =
        new NessieError(
            Status.UNAUTHORIZED.getCode(),
            Status.UNAUTHORIZED.getReason(),
            "Could not parse error object in response.",
            new RuntimeException("Could not parse error object in response."));
    assertThatThrownBy(
            () ->
                ResponseCheckFilter.checkResponse(
                    new TestResponseContext(Status.UNAUTHORIZED, null), MAPPER))
        .isInstanceOf(NessieServiceException.class)
        .extracting("error")
        .isEqualTo(defaultError);
  }

  @Test
  void testGood() {
    assertDoesNotThrow(
        () -> ResponseCheckFilter.checkResponse(new TestResponseContext(Status.OK, null), MAPPER));
  }

  private static Stream<Arguments> provider() {
    return Stream.of(
        Arguments.of(Status.BAD_REQUEST, NessieBadRequestException.class),
        Arguments.of(Status.UNAUTHORIZED, NessieNotAuthorizedException.class),
        Arguments.of(Status.FORBIDDEN, NessieForbiddenException.class),
        Arguments.of(Status.NOT_FOUND, NessieNotFoundException.class),
        Arguments.of(Status.CONFLICT, NessieConflictException.class),
        Arguments.of(Status.INTERNAL_SERVER_ERROR, NessieInternalServerException.class));
  }

  private static class TestResponseContext implements ResponseContext {

    private final Status code;
    private final NessieError error;

    TestResponseContext(Status code, NessieError error) {
      this.code = code;
      this.error = error;
    }

    @Override
    public Status getResponseCode() {
      return code;
    }

    @Override
    public InputStream getInputStream() {
      Assertions.fail();
      return null;
    }

    @Override
    public InputStream getErrorStream() throws IOException {
      if (error == null) {
        return null;
      }
      String value = MAPPER.writeValueAsString(error);
      return new StringInputStream(value);
    }
  }
}
