/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.services.rest.exceptions;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;
import org.projectnessie.services.config.ExceptionConfig;
import tools.jackson.core.JsonParser;
import tools.jackson.core.exc.StreamReadException;
import tools.jackson.databind.DatabindException;

class TestJackson3ExceptionMappers {

  @Test
  void streamReadExceptionMapsToBadRequest() {
    NessieJaxRsJackson3StreamReadExceptionMapper mapper =
        init(new NessieJaxRsJackson3StreamReadExceptionMapper());

    Response response = mapper.toResponse(new StreamReadException("invalid JSON"));

    assertBadRequest(response, "invalid JSON");
  }

  @Test
  void databindExceptionMapsToBadRequest() {
    NessieJaxRsJackson3DatabindExceptionMapper mapper =
        init(new NessieJaxRsJackson3DatabindExceptionMapper());

    Response response =
        mapper.toResponse(DatabindException.from((JsonParser) null, "invalid shape"));

    assertBadRequest(response, "invalid shape");
  }

  @Test
  void catchAllMapperHandlesJackson3ExceptionsAsBadRequest() {
    NessieExceptionMapper mapper = init(new NessieExceptionMapper());

    Response response = mapper.toResponse(new StreamReadException("invalid JSON"));

    assertBadRequest(response, "invalid JSON");
  }

  private static <T extends BaseExceptionMapper<?>> T init(T mapper) {
    setField(mapper, "config", new TestExceptionConfig());
    mapper.headers = EmptyHttpHeaders.INSTANCE;
    return mapper;
  }

  private static void setField(Object target, String name, Object value) {
    try {
      Field field = BaseExceptionMapper.class.getDeclaredField(name);
      field.setAccessible(true);
      field.set(target, value);
    } catch (ReflectiveOperationException e) {
      throw new LinkageError(e.getMessage(), e);
    }
  }

  private static void assertBadRequest(Response response, String message) {
    assertThat(response.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    NessieError error = (NessieError) response.getEntity();
    assertThat(error.getMessage()).contains(message);
    assertThat(error.getStatus()).isEqualTo(Response.Status.BAD_REQUEST.getStatusCode());
    assertThat(error.getReason()).isEqualTo(Response.Status.BAD_REQUEST.getReasonPhrase());
    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.BAD_REQUEST);
  }

  private static final class TestExceptionConfig implements ExceptionConfig {
    @Override
    public boolean sendStacktraceToClient() {
      return false;
    }
  }

  private enum EmptyHttpHeaders implements HttpHeaders {
    INSTANCE;

    @Override
    public List<String> getRequestHeader(String name) {
      return List.of();
    }

    @Override
    public String getHeaderString(String name) {
      return null;
    }

    @Override
    public boolean containsHeaderString(
        String name, String valueSeparatorRegex, Predicate<String> valuePredicate) {
      return false;
    }

    @Override
    public MultivaluedMap<String, String> getRequestHeaders() {
      return null;
    }

    @Override
    public List<MediaType> getAcceptableMediaTypes() {
      return List.of();
    }

    @Override
    public List<Locale> getAcceptableLanguages() {
      return List.of();
    }

    @Override
    public MediaType getMediaType() {
      return null;
    }

    @Override
    public Locale getLanguage() {
      return null;
    }

    @Override
    public Map<String, Cookie> getCookies() {
      return Map.of();
    }

    @Override
    public Date getDate() {
      return null;
    }

    @Override
    public int getLength() {
      return -1;
    }
  }
}
