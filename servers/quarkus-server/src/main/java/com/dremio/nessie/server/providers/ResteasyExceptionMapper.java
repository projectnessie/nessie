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

import javax.enterprise.inject.Alternative;
import javax.validation.ValidationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.jboss.resteasy.api.validation.ResteasyViolationException;
import org.jboss.resteasy.api.validation.Validation;

/**
 * "Special" implementation for exceptions that extend {@link ValidationException}, as those
 * do not "go through" {@link NessieExceptionMapper} and there need to be two
 * {@link ExceptionMapper} beans for the Nessie-server.
 */
// Use our exception-mapper instead of io.quarkus.hibernate.validator.runtime.jaxrs.ResteasyViolationExceptionMapper
@Alternative
@Provider
public class ResteasyExceptionMapper
    extends BaseExceptionMapper
    implements ExceptionMapper<ValidationException> {

  @Override
  public Response toResponse(ValidationException exception) {
    if (exception instanceof ResteasyViolationException) {
      ResteasyViolationException violationException = (ResteasyViolationException) exception;
      Exception e = violationException.getException();
      if (e == null) {
        boolean returnValueViolation = !violationException.getReturnValueViolations().isEmpty();
        Status st = returnValueViolation ? Status.INTERNAL_SERVER_ERROR : Status.BAD_REQUEST;
        return buildExceptionResponse(
            st.getStatusCode(),
            st.getReasonPhrase(),
            exception.getMessage(),
            exception,
            false, // no need to send the stack trace for a validation-error
            b -> b.header(Validation.VALIDATION_HEADER, "true"));
      }
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
