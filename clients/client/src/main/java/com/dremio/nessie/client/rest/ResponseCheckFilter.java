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
package com.dremio.nessie.client.rest;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.core.Response.Status;

import com.dremio.nessie.error.ImmutableNessieError;
import com.dremio.nessie.error.NessieError;
import com.dremio.nessie.json.ObjectMapperBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;

public class ResponseCheckFilter implements ClientResponseFilter {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperBuilder.createObjectMapper();

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) throws IOException {
    checkResponse(responseContext.getStatus(), responseContext);
  }

  /**
   * check that response had a valid return code. Throw exception if not.
   */
  public static void checkResponse(int statusCode, ClientResponseContext response) {
    Status status = Status.fromStatusCode(statusCode);
    if (status == Status.OK || status == Status.CREATED) {
      return;
    }
    NessieError error = readException(status, response);
    switch (status) {
      case BAD_REQUEST:
        throw new NessieBadRequestException(error);
      case UNAUTHORIZED:
        throw new NessieNotAuthorizedException(error);
      case FORBIDDEN:
        throw new NessieForbiddenException(error);
      case NOT_FOUND:
        throw new NessieNotFoundException(error);
      case CONFLICT:
        throw new NessieConflictException(error);
      case PRECONDITION_FAILED:
        throw new NessiePreconditionFailedException(error);
      case INTERNAL_SERVER_ERROR:
        throw new NessieInternalServerException(error);
      default:
        try {
          String msg = OBJECT_MAPPER.writeValueAsString(error);
          throw new RuntimeException(String.format("Unknown exception %s with message %s", status, msg));
        } catch (JsonProcessingException e) {
          throw new RuntimeException(String.format("Unknown exception %s", status), e);
        }
    }
  }

  private static NessieError readException(Status status, ClientResponseContext response) {
    InputStream inputStream = response.getEntityStream();
    NessieError error;
    String msg;
    try (Reader reader = new InputStreamReader(inputStream)) {
      msg = CharStreams.toString(reader);
    } catch (IOException exception) {
      msg = Throwables.getStackTraceAsString(exception);
    }
    try {
      error = OBJECT_MAPPER.readValue(msg, NessieError.class);
    } catch (Exception ex) {
      error = ImmutableNessieError.builder()
                                  .errorCode(status.getStatusCode())
                                  .errorMessage(msg)
                                  .statusMessage(status.getReasonPhrase())
                                  .build();
    }
    return error;
  }

}
