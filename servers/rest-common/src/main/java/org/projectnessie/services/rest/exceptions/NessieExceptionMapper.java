/*
 * Copyright (C) 2023 Dremio
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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.util.stream.Collectors;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Default" exception mapper implementations, mostly used to serialize the {@link
 * BaseNessieClientServerException Nessie-exceptions} as JSON consumable by Nessie client
 * implementations. Does also map other, non-{@link BaseNessieClientServerException}s as HTTP/503
 * (internal server errors) with a JSON-serialized {@link org.projectnessie.error.NessieError}.
 */
@Provider
@ApplicationScoped
public class NessieExceptionMapper extends BaseExceptionMapper<Exception> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieExceptionMapper.class);

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public NessieExceptionMapper() {}

  @Override
  public Response toResponse(Exception exception) {
    ErrorCode errorCode;
    String message;

    if (exception instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) exception;
      errorCode = e.getErrorCode();
      message = exception.getMessage();
    } else if (exception.getCause() instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) exception.getCause();
      errorCode = e.getErrorCode();
      message = exception.getCause().getMessage();
    } else if (exception instanceof JsonParseException
        || exception instanceof JsonMappingException
        || exception instanceof IllegalArgumentException) {
      errorCode = ErrorCode.BAD_REQUEST;
      message = exception.getMessage();
    } else if (exception instanceof WebApplicationException) {
      if (exception.getCause() instanceof IllegalArgumentException
          && ((WebApplicationException) exception).getResponse().getStatus() == 404) {
        errorCode = ErrorCode.BAD_REQUEST;
        message = exception.getCause().getMessage();
      } else {
        return ((WebApplicationException) exception).getResponse();
      }
    } else {
      LOGGER.warn("Unhandled exception returned as HTTP/500 to client", exception);
      errorCode = ErrorCode.UNKNOWN;
      message =
          Throwables.getCausalChain(exception).stream()
              .map(Throwable::toString)
              .collect(Collectors.joining(", caused by "));
    }

    return buildExceptionResponse(errorCode, message, exception);
  }
}
