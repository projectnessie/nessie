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
package com.dremio.nessie.server.providers;

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.error.BaseNessieClientServerException;
import com.dremio.nessie.error.NessieError;
import com.dremio.nessie.services.config.ServerConfig;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;

@Provider
public class NessieExceptionMapper implements ExceptionMapper<Exception> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieExceptionMapper.class);

  @Inject
  ServerConfig serverConfig;

  @Override
  public Response toResponse(Exception exception) {

    if (exception instanceof WebApplicationException) {
      Response.Status status = Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
      return exception(status, exception.getMessage(), exception);
    } else if (exception instanceof BaseNessieClientServerException) {
      // log message at debug level so we can review stack traces if enabled.
      LOGGER.debug("Exception on server with appropriate error sent to client.", exception);
      BaseNessieClientServerException e = (BaseNessieClientServerException) exception;
      return exception(e.getStatus(), e.getReason(), exception.getMessage(), exception);
    } else if (exception instanceof JsonParseException) {
      return exception(Status.BAD_REQUEST, exception.getMessage(), exception);
    } else if (exception instanceof JsonMappingException) {
      return exception(Status.BAD_REQUEST, exception.getMessage(), exception);
    } else {
      return exception(Status.INTERNAL_SERVER_ERROR, exception.getMessage(), exception);
    }
  }

  private Response exception(int status, String reason, String message, Exception e) {
    String stack = serverConfig.shouldSendstackTraceToAPIClient() ? Throwables.getStackTraceAsString(e) : null;
    NessieError error = new NessieError(message, status, reason, stack);
    LOGGER.debug("Failure on server, propagated to client. Status: {} {}, Message: {}.", status, reason, message, e);
    return Response.status(status).entity(error).type(MediaType.APPLICATION_JSON_TYPE).build();
  }

  private Response exception(Status status, String message, Exception e) {
    return exception(status.getStatusCode(), status.getReasonPhrase(), message, e);
  }

}
