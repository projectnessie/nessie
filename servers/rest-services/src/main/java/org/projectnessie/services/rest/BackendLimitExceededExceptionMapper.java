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
package org.projectnessie.services.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.services.rest.exceptions.BaseExceptionMapper;
import org.projectnessie.versioned.BackendLimitExceededException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
@ApplicationScoped
public class BackendLimitExceededExceptionMapper
    extends BaseExceptionMapper<BackendLimitExceededException> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BackendLimitExceededExceptionMapper.class);

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public BackendLimitExceededExceptionMapper() {}

  @Override
  public Response toResponse(BackendLimitExceededException exception) {
    LOGGER.warn("Backend throttled/refused the request: {}", exception.toString());
    ErrorCode errorCode = ErrorCode.TOO_MANY_REQUESTS;
    String message = "Backend store refused to process the request: " + exception;
    return buildExceptionResponse(errorCode, message, exception);
  }
}
