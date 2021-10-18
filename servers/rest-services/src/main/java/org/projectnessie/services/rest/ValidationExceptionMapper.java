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
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.ValidationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.projectnessie.services.config.ServerConfig;

/**
 * "Special" implementation for exceptions that extend {@link ValidationException}, as those do not
 * "go through" {@link NessieExceptionMapper} and there need to be two {@link ExceptionMapper} beans
 * for the Nessie-server.
 */
@Provider
public class ValidationExceptionMapper extends BaseExceptionMapper<ValidationException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public ValidationExceptionMapper() {
    this(null);
  }

  @Inject
  public ValidationExceptionMapper(ServerConfig config) {
    super(config);
  }

  @Override
  public Response toResponse(ValidationException exception) {
    if (exception instanceof ConstraintViolationException) {
      final ConstraintViolationException cve = (ConstraintViolationException) exception;
      Status status = Response.Status.BAD_REQUEST;
      for (ConstraintViolation<?> violation : cve.getConstraintViolations()) {
        for (final Path.Node node : violation.getPropertyPath()) {
          final ElementKind kind = node.getKind();

          if (ElementKind.RETURN_VALUE.equals(kind)) {
            status = Response.Status.INTERNAL_SERVER_ERROR;
          }
        }
      }
      return buildExceptionResponse(
          status.getStatusCode(),
          status.getReasonPhrase(),
          exception.getMessage(),
          exception,
          false, // no need to send the stack trace for a validation-error
          header -> {});
    }

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

  private void doUnwrapException(StringBuffer sb, Throwable t) {
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
