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
import javax.validation.ConstraintViolationException;
import javax.validation.ValidationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.projectnessie.services.config.ServerConfig;

/**
 * "Special" implementation for exceptions that extend {@link ValidationException}, as those
 * do not "go through" {@link NessieExceptionMapper} and there need to be two
 * {@link ExceptionMapper} beans for the Nessie-server.
 */
@Provider
public class ValidationExceptionMapper
    extends BaseExceptionMapper
    implements ExceptionMapper<ValidationException> {

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
  public ValidationExceptionMapper() {
    // empty
  }

  /**
   * Constructor for non-CDI/Quarkus usage.
   * @param config Nessie server-config
   */
  @SuppressWarnings("unused")
  public ValidationExceptionMapper(ServerConfig config) {
    this.config = config;
  }

  @Override
  protected ServerConfig getConfig() {
    return config;
  }

  @Override
  public Response toResponse(ValidationException exception) {
    return buildExceptionResponse(
        Status.INTERNAL_SERVER_ERROR.getStatusCode(),
        Status.INTERNAL_SERVER_ERROR.getReasonPhrase(),
        unwrapException(exception),
        exception);
  }

  protected String unwrapException(Throwable t) {
    StringBuffer sb = new StringBuffer();
    doUnwrapException(sb, t);
    return sb.toString();
  }
}
