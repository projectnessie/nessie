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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.error.ImmutableNessieError;
import com.dremio.nessie.error.ImmutableNessieError.Builder;
import com.dremio.nessie.versioned.ReferenceAlreadyExistsException;
import com.dremio.nessie.versioned.ReferenceConflictException;
import com.dremio.nessie.versioned.ReferenceNotFoundException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.common.base.Throwables;

@Provider
public class ErrorMapper implements ExceptionMapper<Exception> {
  private static final Logger logger = LoggerFactory.getLogger(ErrorMapper.class);

  @Override
  public Response toResponse(Exception exception) {
    if (exception instanceof WebApplicationException) {
      Response.Status status = Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
      return exception(status, exception.getMessage(), exception);
    } else if (exception instanceof JsonParseException) {
      return exception(Status.BAD_REQUEST, exception.getMessage(), exception);
    } else if (exception instanceof JsonMappingException) {
      return exception(Status.BAD_REQUEST, exception.getMessage(), exception);
    } else if (exception instanceof ReferenceNotFoundException) {
      return exception(Response.Status.NOT_FOUND, "ref not found", exception);
    } else if (exception instanceof ReferenceConflictException) {
      return exception(Response.Status.PRECONDITION_FAILED, "Tag not up to date", exception);
    } else if (exception instanceof ReferenceAlreadyExistsException) {
      return exception(Response.Status.CONFLICT, "ref already exists", exception);
    } else {
      exception.printStackTrace();
      return exception(Status.INTERNAL_SERVER_ERROR, exception.getMessage(), exception);
    }
  }

  private static Response exception(Response.Status status,
                                    String message,
                                    Exception e) {
    Builder builder = ImmutableNessieError.builder()
                                          .errorCode(status.getStatusCode())
                                          .errorMessage(message)
                                          .statusMessage(status.getReasonPhrase());
    if (e != null) {
      builder.stackTrace(Throwables.getStackTraceAsString(e));
    }
    logger.debug(String.format("Request failed with status code %s", status.getStatusCode()), e);
    return Response.status(status)
                   .entity(Entity.entity(builder.build(), MediaType.APPLICATION_JSON_TYPE))
                   .build();
  }

}
