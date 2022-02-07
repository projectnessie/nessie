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
package org.projectnessie.services.rest;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;
import java.security.AccessControlException;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * "Default" exception mapper implementations, mostly used to serialize the {@link
 * BaseNessieClientServerException Nessie-exceptions} as JSON consumable by Nessie client
 * implementations. Does also map other, non-{@link BaseNessieClientServerException}s as HTTP/503
 * (internal server errors) with a JSON-serialized {@link org.projectnessie.error.NessieError}.
 */
@Provider
public class NessieExceptionMapper extends BaseExceptionMapper<Exception> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieExceptionMapper.class);

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public NessieExceptionMapper() {
    this(null);
  }

  @Inject
  public NessieExceptionMapper(ServerConfig config) {
    super(config);
  }

  @Override
  public Response toResponse(Exception exception) {
    ErrorCode errorCode;
    String message;

    if (exception instanceof BaseNessieClientServerException) {
      BaseNessieClientServerException e = (BaseNessieClientServerException) exception;
      errorCode = e.getErrorCode();
      message = exception.getMessage();
    } else if (exception instanceof JsonParseException
        || exception instanceof JsonMappingException
        || exception instanceof IllegalArgumentException) {
      errorCode = ErrorCode.BAD_REQUEST;
      message = exception.getMessage();
    } else if (exception instanceof BackendLimitExceededException) {
      LOGGER.warn("Backend throttled/refused the request: {}", exception.toString());
      errorCode = ErrorCode.TOO_MANY_REQUESTS;
      message = "Backend store refused to process the request: " + exception;
    } else if (exception instanceof AccessControlException) {
      errorCode = ErrorCode.FORBIDDEN;
      message = exception.getMessage();
    } else {
      LOGGER.warn("Unhandled exception returned as HTTP/500 to client", exception);
      errorCode = ErrorCode.UNKNOWN;
      message =
          Throwables.getCausalChain(exception).stream()
              .map(Throwable::toString)
              .collect(Collectors.joining(", caused by"));
    }

    return buildExceptionResponse(errorCode, message, exception);
  }
}
