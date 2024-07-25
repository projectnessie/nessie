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

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import io.quarkus.resteasy.reactive.jackson.runtime.serialisers.FullyFeaturedServerJacksonMessageBodyReader;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.NessieError;
import org.projectnessie.services.rest.exceptions.BaseExceptionMapper;

/**
 * Maps a {@link WebApplicationException} to return a "proper" {@link NessieError} in the case when
 * the request body fails JSON parsing.
 *
 * <p>The behavior in <em>quarkus-resteasy-reactive</em> is different from non-reactive resteasy,
 * because, as of Quarkus 3.9.4, {@link FullyFeaturedServerJacksonMessageBodyReader#readFrom(Class,
 * Type, Annotation[], MediaType, MultivaluedMap, InputStream)} throws a {@link
 * WebApplicationException} for {@link DatabindException} ({@link StreamReadException}), which makes
 * it impossible to install an exception mapper for those, so we have to install one for {@link
 * WebApplicationException}.
 *
 * <p>See also {@link MismatchedInputExceptionMapper} for a special case of a "bad request".
 */
@Provider
@ApplicationScoped
public class WebApplicationExceptionRemapper extends BaseExceptionMapper<WebApplicationException> {
  @Override
  public Response toResponse(WebApplicationException exception) {
    Response response = exception.getResponse();
    if (response.getStatus() != ErrorCode.BAD_REQUEST.httpStatus()
        || response.getEntity() != null) {
      return exception.getResponse();
    }

    Throwable cause = exception.getCause();
    String message = cause != null ? cause.getMessage() : exception.getMessage();

    return buildExceptionResponse(
        ErrorCode.BAD_REQUEST,
        message,
        exception,
        false, // no need to send the stack trace for a validation-error
        b -> {});
  }
}
