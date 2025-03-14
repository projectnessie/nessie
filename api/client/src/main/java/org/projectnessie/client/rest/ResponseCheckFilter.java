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
package org.projectnessie.client.rest;

import static org.projectnessie.client.http.Status.INTERNAL_SERVER_ERROR_CODE;
import static org.projectnessie.client.http.Status.SERVICE_UNAVAILABLE_CODE;
import static org.projectnessie.client.http.Status.UNAUTHORIZED_CODE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.rest.io.CapturingInputStream;
import org.projectnessie.error.ErrorCode;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieUnavailableException;

public class ResponseCheckFilter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * check that response had a valid return code. Throw exception if not.
   *
   * @param con open http connection
   * @throws IOException Throws IOException for certain error types.
   */
  public static void checkResponse(ResponseContext con) throws Exception {
    final Status status;
    final NessieError error;
    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    status = con.getStatus();
    if (status.getCode() > 199 && status.getCode() < 300) {
      return;
    }

    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    try (InputStream is = con.getInputStream()) {
      error = decodeErrorObject(status, is, MAPPER.reader());
    }

    Optional<Exception> modelException = ErrorCode.asException(error);
    if (modelException.isPresent()) {
      // expected exception reported by one of the Nessie Server API endpoints
      throw modelException.get();
    }

    // If the error could not be identified as a Nessie-controlled error, throw a client-side
    // exception with some level of break-down by sub-class to allow for intelligent exception
    // handling on the caller side.
    Exception exception;
    switch (status.getCode()) {
      case INTERNAL_SERVER_ERROR_CODE:
        exception = new NessieInternalServerException(error);
        break;
      case SERVICE_UNAVAILABLE_CODE:
        exception = new NessieUnavailableException(error);
        break;
      case UNAUTHORIZED_CODE:
        // Note: UNAUTHORIZED at this point cannot be a Nessie-controlled error.
        // It must be an error reported at a higher level HTTP service in front of the Nessie
        // Server.
        exception = new NessieNotAuthorizedException(error);
        break;
      default:
        // Note: Non-Nessie 404 (Not Found) errors (e.g. mistakes in Nessie URIs) will also go
        // through this code and will be reported as generic NessieServiceException.
        exception = new NessieServiceException(error);
    }

    throw exception;
  }

  private static NessieError decodeErrorObject(
      Status status, InputStream inputStream, ObjectReader reader) {
    NessieError error;
    if (inputStream == null) {
      error =
          ImmutableNessieError.builder()
              .errorCode(ErrorCode.UNKNOWN)
              .status(status.getCode())
              .reason(status.getReason())
              .message("Could not parse error object in response.")
              .clientProcessingError("Could not parse error object in response.")
              .build();
    } else {
      CapturingInputStream capturing = new CapturingInputStream(inputStream);
      try {
        JsonNode errorData = reader.readTree(capturing);
        error = reader.treeToValue(errorData, NessieError.class);
        // There's been a behavior change in Jackson between versions 2.18.2 and 2.18.3.
        // Up to 2.18.2, `error` was non-`null` even if `errorData` is empty,
        // singe 2.18.3, `error` is `null` if `errorData` is empty.
        if (error == null) {
          error = noResponseError(status, "(no response from server)", "");
        }
      } catch (IOException e) {
        // The error payload is valid JSON (or an empty response), but does not represent a
        // NessieError.
        //
        // This can happen when the endpoint is not a Nessie REST API endpoint, but for example an
        // OAuth2 service.
        //
        // Report the captured source input just in case, but do not populate
        // `clientProcessingException`, because that would put the Jackson exception stack trace
        // in the message of the (later) thrown exception, which is likely to be displayed to
        // users. Humans get confused when they see stack traces, even if the reason's legit, for
        // example an authentication failure.
        //
        String cap = capturing.captured().trim();
        error = noResponseError(status, e.toString(), cap);
      }
    }
    return error;
  }

  private static ImmutableNessieError noResponseError(
      Status status, String clientProcessingError, String cap) {
    return ImmutableNessieError.builder()
        .message(
            cap.isEmpty()
                ? "got empty response body from server"
                : ("Could not parse error object in response beginning with: " + cap))
        .status(status.getCode())
        .reason(status.getReason())
        .clientProcessingError(clientProcessingError)
        .build();
  }
}
