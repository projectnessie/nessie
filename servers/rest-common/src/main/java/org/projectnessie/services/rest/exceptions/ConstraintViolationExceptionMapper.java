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
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.ElementKind;
import jakarta.validation.Path;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.error.ErrorCode;

/**
 * "Special" implementation for exceptions that extend {@link ConstraintViolationException}, as
 * those do not "go through" {@link NessieExceptionMapper} and also not through the {@link
 * ValidationExceptionMapper}.
 */
@Provider
@ApplicationScoped
public class ConstraintViolationExceptionMapper
    extends BaseExceptionMapper<ConstraintViolationException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public ConstraintViolationExceptionMapper() {}

  @Override
  public Response toResponse(ConstraintViolationException exception) {
    ErrorCode errorCode = ErrorCode.BAD_REQUEST;
    for (ConstraintViolation<?> violation : exception.getConstraintViolations()) {
      for (final Path.Node node : violation.getPropertyPath()) {
        if (ElementKind.RETURN_VALUE == node.getKind()) {
          errorCode = ErrorCode.UNKNOWN; // translates to "internal server error"
        }
      }
    }

    // Construct the user-level message the same way for all validator implementations.
    ConstraintViolationException canonical =
        new ConstraintViolationException(exception.getConstraintViolations());

    return buildExceptionResponse(
        errorCode, canonical.getMessage(), exception, false, header -> {});
  }
}
