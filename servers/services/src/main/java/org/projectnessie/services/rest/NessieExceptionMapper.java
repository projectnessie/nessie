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

import javax.inject.Inject;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.services.config.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * "Default" exception mapper implementations, mostly used to serialize the
 * {@link BaseNessieClientServerException Nessie-exceptions}  as JSON consumable by Nessie
 * client implementations. Does also map other, non-{@link BaseNessieClientServerException}s
 * as HTTP/503 (internal server errors) with a JSON-serialized
 * {@link org.projectnessie.error.NessieError}.
 */
@Provider
public class NessieExceptionMapper
    extends BaseExceptionMapper
    implements ExceptionMapper<Exception> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NessieExceptionMapper.class);

  @Inject
  ServerConfig config;

  /**
   * Public no-arg constructor for CDI/Quarkus.
   * <p>This public no-arg constructor is required for "proper" code-coverage.</p>
   * <p>Without a public no-arg constructor, Quarkus "injects" one and then jacoco
   * can no longer associate the class and as a result code-coverage for this class
   * will not be available.</p>
   * <p>See also: <a href="https://issues.jboss.org/browse/RESTEASY-1538">RESTEASY-1538</a></p>
   */
  @SuppressWarnings("unused")
  public NessieExceptionMapper() {
    // intentionally empty
  }

  /**
   * Constructor for non-CDI/Quarkus usage.
   * @param config Nessie server-config
   */
  @SuppressWarnings("unused")
  public NessieExceptionMapper(ServerConfig config) {
    this.config = config;
  }

  @Override
  protected ServerConfig getConfig() {
    return config;
  }

  @Override
  public Response toResponse(Exception exception) {
    int status;
    String reason;

    if (exception instanceof WebApplicationException) {
      WebApplicationException e = (WebApplicationException) exception;
      Status st = Status.fromStatusCode(e.getResponse().getStatus());
      status = st.getStatusCode();
      reason = st.getReasonPhrase();
    } else if (exception instanceof BaseNessieClientServerException) {
      // log message at debug level so we can review stack traces if enabled.
      LOGGER.debug("Exception on server with appropriate error sent to client.", exception);
      BaseNessieClientServerException e = (BaseNessieClientServerException) exception;
      status = e.getStatus();
      reason = e.getReason();
    } else if (exception instanceof JsonParseException
        || exception instanceof JsonMappingException) {
      status = Status.BAD_REQUEST.getStatusCode();
      reason = Status.BAD_REQUEST.getReasonPhrase();
    } else {
      status = Status.INTERNAL_SERVER_ERROR.getStatusCode();
      reason = Status.INTERNAL_SERVER_ERROR.getReasonPhrase();
    }

    return buildExceptionResponse(status, reason, exception.getMessage(), exception);
  }
}
