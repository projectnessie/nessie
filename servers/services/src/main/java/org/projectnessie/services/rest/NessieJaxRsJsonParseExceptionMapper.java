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

import com.fasterxml.jackson.core.JsonParseException;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.Provider;
import org.projectnessie.services.config.ServerConfig;

/**
 * "Special" implementation for exceptions that extend {@link JsonParseException} that is needed
 * when using Jackson-JaxRs, as those do not "go through" {@link NessieExceptionMapper}, because
 * Jackson-JaxRs provides its own mapper for {@link JsonParseException}.
 */
@Provider
public class NessieJaxRsJsonParseExceptionMapper extends BaseExceptionMapper<JsonParseException> {

  // Unused constructor
  // Required because of https://issues.jboss.org/browse/RESTEASY-1538
  public NessieJaxRsJsonParseExceptionMapper() {
    this(null);
  }

  @Inject
  public NessieJaxRsJsonParseExceptionMapper(ServerConfig config) {
    super(config);
  }

  @Override
  public Response toResponse(JsonParseException exception) {
    return buildExceptionResponse(
        Status.BAD_REQUEST.getStatusCode(),
        Status.BAD_REQUEST.getReasonPhrase(),
        exception.getMessage(),
        exception);
  }
}
