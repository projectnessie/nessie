/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.rest.exceptions;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.validation.ValidationException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.error.ErrorCode;

/**
 * "Special" implementation for exceptions that extend {@link ValidationException}, as those do not
 * "go through" {@link NessieExceptionMapper} and there need to be two {@link ExceptionMapper} beans
 * for the Nessie-server.
 */
@Provider
@ApplicationScoped
public class ValidationExceptionMapper extends BaseExceptionMapper<ValidationException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public ValidationExceptionMapper() {}

  @Override
  public Response toResponse(ValidationException exception) {
    return buildExceptionResponse(ErrorCode.UNKNOWN, unwrapException(exception), exception);
  }

  protected String unwrapException(Throwable t) {
    StringBuilder sb = new StringBuilder();
    doUnwrapException(sb, t);
    return sb.toString();
  }

  private void doUnwrapException(StringBuilder sb, Throwable t) {
    if (t == null) {
      return;
    }
    sb.append(t);
    if (t.getCause() != null && t != t.getCause()) {
      sb.append('[');
      doUnwrapException(sb, t.getCause());
      sb.append(']');
    }
  }
}
