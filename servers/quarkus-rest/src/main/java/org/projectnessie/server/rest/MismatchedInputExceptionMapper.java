/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.server.rest;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.services.rest.exceptions.BaseExceptionMapper;

/**
 * "Special" implementation for {@link MismatchedInputException}s that represent bad input
 * parameters.
 *
 * <p>See also {@link WebApplicationExceptionRemapper} for a special case of a "bad request".
 */
@Provider
@ApplicationScoped
public class MismatchedInputExceptionMapper extends BaseExceptionMapper<MismatchedInputException> {
  @Override
  public Response toResponse(MismatchedInputException exception) {
    return buildExceptionResponse(
        ErrorCode.BAD_REQUEST,
        exception.getMessage(),
        exception,
        false, // no need to send the stack trace for a validation-error
        b -> {});
  }
}
