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
package org.projectnessie.server.providers;

import javax.inject.Inject;
import javax.validation.ValidationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.jboss.resteasy.api.validation.ResteasyViolationException;
import org.jboss.resteasy.api.validation.Validation;
import org.projectnessie.services.config.ServerConfig;
import org.projectnessie.services.rest.BaseExceptionMapper;
import org.projectnessie.services.rest.NessieExceptionMapper;

/**
 * "Special" implementation for exceptions that extend {@link ValidationException}, as those do not
 * "go through" {@link NessieExceptionMapper} and there need to be two {@link ExceptionMapper} beans
 * for the Nessie-server.
 */
// Use our exception-mapper instead of
// io.quarkus.hibernate.validator.runtime.jaxrs.ResteasyViolationExceptionMapper
@Provider
public class ResteasyExceptionMapper extends BaseExceptionMapper<ResteasyViolationException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public ResteasyExceptionMapper() {
    this(null);
  }

  @Inject
  public ResteasyExceptionMapper(ServerConfig config) {
    super(config);
  }

  @Override
  public Response toResponse(ResteasyViolationException exception) {
    Exception e = exception.getException();
    if (e == null) {
      boolean returnValueViolation = !exception.getReturnValueViolations().isEmpty();
      Status st = returnValueViolation ? Status.INTERNAL_SERVER_ERROR : Status.BAD_REQUEST;
      return buildExceptionResponse(
          st.getStatusCode(),
          st.getReasonPhrase(),
          exception.getMessage(),
          exception,
          false, // no need to send the stack trace for a validation-error
          b -> b.header(Validation.VALIDATION_HEADER, "true"));
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
