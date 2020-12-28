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
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.error.BaseNessieClientServerException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * "Default" exception mapper implementations, mostly used to serialize the
 * {@link BaseNessieClientServerException Nessie-exceptions}  as JSON consumable by Nessie
 * client implementations. Does also map other, non-{@link BaseNessieClientServerException}s
 * as HTTP/503 (internal server errors) with a JSON-serialized
 * {@link com.dremio.nessie.error.NessieError}.
 */
@Provider
public class NessieExceptionMapper
    extends BaseExceptionMapper
    implements ExceptionMapper<Exception> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieExceptionMapper.class);

  @Override
  public Response toResponse(Exception exception) {
    Response.Status status;

    if (exception instanceof WebApplicationException) {
      status = Status.fromStatusCode(((WebApplicationException) exception).getResponse().getStatus());
    } else if (exception instanceof BaseNessieClientServerException) {
      // log message at debug level so we can review stack traces if enabled.
      LOGGER.debug("Exception on server with appropriate error sent to client.", exception);
      status = ((BaseNessieClientServerException) exception).getStatus();
    } else if (exception instanceof JsonParseException) {
      status = Status.BAD_REQUEST;
    } else if (exception instanceof JsonMappingException) {
      status = Status.BAD_REQUEST;
    } else {
      status = Status.INTERNAL_SERVER_ERROR;
    }

    return buildExceptionResponse(status, exception.getMessage(), exception);
  }
}
