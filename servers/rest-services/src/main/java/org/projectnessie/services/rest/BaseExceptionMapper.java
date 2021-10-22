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

import com.google.common.base.Throwables;
import java.util.function.Consumer;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;
import org.projectnessie.services.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Code shared between concrete exception-mapper implementations. */
public abstract class BaseExceptionMapper<T extends Throwable> implements ExceptionMapper<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExceptionMapper.class);

  private final ServerConfig serverConfig;

  protected BaseExceptionMapper(ServerConfig serverConfig) {
    this.serverConfig = serverConfig;
  }

  protected Response buildExceptionResponse(
      int status, String reason, String message, Exception e) {

    return buildExceptionResponse(
        status, reason, message, e, serverConfig.sendStacktraceToClient(), h -> {});
  }

  protected Response buildExceptionResponse(
      int status,
      String reason,
      String message,
      Exception e,
      boolean includeExceptionStackTrace,
      Consumer<ResponseBuilder> responseHandler) {

    String stack = includeExceptionStackTrace ? Throwables.getStackTraceAsString(e) : null;

    ErrorCode errorCode = ErrorCode.UNKNOWN;
    if (e instanceof BaseNessieClientServerException) {
      errorCode = ((BaseNessieClientServerException) e).getErrorCode();
    }

    NessieError error =
        ImmutableNessieError.builder()
            .message(message)
            .status(status)
            .errorCode(errorCode)
            .reason(reason)
            .serverStackTrace(stack)
            .build();
    LOGGER.debug(
        "Failure on server, propagated to client. Status: {} {}, Message: {}.",
        status,
        reason,
        message,
        e);
    ResponseBuilder responseBuilder =
        Response.status(status).entity(error).type(MediaType.APPLICATION_JSON_TYPE);
    responseHandler.accept(responseBuilder);
    return responseBuilder.build();
  }
}
