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

import javax.ws.rs.client.ClientRequestContext;
import javax.ws.rs.client.ClientResponseContext;
import javax.ws.rs.client.ClientResponseFilter;
import javax.ws.rs.core.Response.Status;

import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieError;
import com.dremio.nessie.error.NessieNotFoundException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class ResponseCheckFilter implements ClientResponseFilter {

  private static final ObjectReader READER = new ObjectMapper().readerFor(NessieError.class);

  @Override
  public void filter(ClientRequestContext requestContext, ClientResponseContext responseContext) throws IOException {
    checkResponse(responseContext.getStatus(), responseContext);
  }

  /**
   * check that response had a valid return code. Throw exception if not.
   * @throws IOException Throws IOException for certain error types.
   */
  @SuppressWarnings("incomplete-switch")
  public static void checkResponse(int statusCode, ClientResponseContext cntxt) throws IOException {
    if (statusCode > 199 && statusCode < 300) {
      return;
    }

    NessieError error = decodeErrorObject(cntxt);
    switch (error.getStatus()) {
      case NOT_FOUND:
        throw new NessieNotFoundException(error);
      case CONFLICT:
        throw new NessieConflictException(error);
      case BAD_REQUEST:
        throw new NessieBadRequestException(error);
      case UNAUTHORIZED:
        throw new NessieNotAuthorizedException(error);
      case FORBIDDEN:
        throw new NessieForbiddenException(error);
      case INTERNAL_SERVER_ERROR:
        throw new NessieInternalServerException(error);
      default:
        throw new NessieServiceException(error);
    }

  }

  private static NessieError decodeErrorObject(ClientResponseContext response) {
    Status status = Status.fromStatusCode(response.getStatus());
    InputStream inputStream = response.getEntityStream();
    NessieError error;
    if (inputStream == null) {
      error = new NessieError(status.getReasonPhrase(), status, null, new RuntimeException("Could not parse error object in response."));
    } else {
      try {
        error = READER.readValue(inputStream, NessieError.class);
      } catch (IOException e) {
        error = new NessieError(status.getReasonPhrase(), status, null, e);
      }
    }
    return error;
  }

}
