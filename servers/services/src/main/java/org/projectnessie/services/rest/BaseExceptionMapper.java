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

import java.util.function.Consumer;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.projectnessie.error.NessieError;
import org.projectnessie.services.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * Code shared between concrete exception-mapper implementations.
 */
public abstract class BaseExceptionMapper {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseExceptionMapper.class);

  protected BaseExceptionMapper() {
    // intentioanlly empty
  }

  protected abstract ServerConfig getConfig();

  protected Response buildExceptionResponse(
      int status,
      String reason,
      String message,
      Exception e) {

    return buildExceptionResponse(
        status,
        reason,
        message,
        e,
        getConfig().sendStacktraceToClient(),
        h -> {});
  }

  protected Response buildExceptionResponse(
      int status,
      String reason,
      String message,
      Exception e,
      boolean includeExceptionStackTrace,
      Consumer<ResponseBuilder> responseHandler) {

    String stack = includeExceptionStackTrace ? Throwables.getStackTraceAsString(e) : null;
    NessieError error = new NessieError(message, status, reason, stack);
    LOGGER.debug("Failure on server, propagated to client. Status: {} {}, Message: {}.",
        status, reason, message, e);
    ResponseBuilder responseBuilder = Response.status(status)
        .entity(error)
        .type(MediaType.APPLICATION_JSON_TYPE);
    responseHandler.accept(responseBuilder);
    return responseBuilder.build();
  }

  protected void doUnwrapException(StringBuffer sb, Throwable t) {
    if (t == null) {
      return;
    }
    sb.append(t.toString());
    if (t.getCause() != null && t != t.getCause()) {
      sb.append('[');
      doUnwrapException(sb, t.getCause());
      sb.append(']');
    }
  }
}
