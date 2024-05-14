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
import jakarta.ws.rs.NotSupportedException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import org.projectnessie.error.ErrorCode;

@Provider
@ApplicationScoped
public class NotSupportedExceptionMapper extends BaseExceptionMapper<NotSupportedException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public NotSupportedExceptionMapper() {}

  @Override
  public Response toResponse(NotSupportedException exception) {
    return buildExceptionResponse(
        ErrorCode.UNSUPPORTED_MEDIA_TYPE, exception.getMessage(), exception);
  }
}
