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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
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
    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    final Status status = con.getResponseCode();
    if (status.getCode() > 199 && status.getCode() < 300) {
      return;
    }

    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    final NessieError error;
    try (InputStream is = con.getErrorStream()) {
      error = decodeErrorObject(status, con, is, MAPPER.reader());
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
    switch (status) {
      case INTERNAL_SERVER_ERROR:
        exception = new NessieInternalServerException(error);
        break;
      case SERVICE_UNAVAILABLE:
        exception = new NessieUnavailableException(error);
        break;
      case UNAUTHORIZED:
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
      Status status, ResponseContext requestCtx, InputStream inputStream, ObjectReader reader) {
    ImmutableNessieError.Builder error =
        ImmutableNessieError.builder()
            .status(status.getCode())
            .reason(status.getReason())
            .message(
                requestCtx.getRequestedMethod() + " " + requestCtx.getRequestedUri() + " failed");

    if (inputStream == null) {
      // jdk8.UrlConnectionRequest seems to have a null InputStream for empty responses
      return error
          .clientProcessingException(
              new IOException("HTTP response was empty (inputStream was null)"))
          .build();
    }

    if (!requestCtx.isJsonCompatibleResponse()) {
      final String inputString = tryReadToString(inputStream);
      return error
          .clientProcessingException(
              new IOException(
                  "Response content type was not json. The response body was:\n" + inputString))
          .build();
    }

    final JsonNode errorData;
    try {
      errorData = reader.readTree(inputStream);
    } catch (IOException e) {
      return error
          .clientProcessingException(new IOException("Unable to parse response body as JSON", e))
          .build();
    }

    if (errorData.isMissingNode()) {
      // empty server response
      if (!Status.NOT_FOUND.equals(status)) {
        error.clientProcessingException(new IOException("HTTP response was empty"));
      }
      return error.build();
    }

    try {
      return reader.treeToValue(errorData, NessieError.class);
    } catch (JsonProcessingException e) {
      // If the error payload is valid JSON, but does not represent a NessieError, it is likely
      // produced by Quarkus and contains the server-side logged error ID. Report the raw JSON
      // text to the caller for trouble-shooting.
      return error
          .clientProcessingException(
              new IOException("JSON response has unknown error format:\n" + errorData, e))
          .build();
    }
  }

  private static String tryReadToString(InputStream inputStream) {
    try {
      StringBuilder out = new StringBuilder();
      int bufferSize = 1024;
      char[] buffer = new char[bufferSize];
      Reader in = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
      for (int numRead; (numRead = in.read(buffer, 0, buffer.length)) > 0; ) {
        out.append(buffer, 0, numRead);
        if (out.length() > 3000) {
          out.append("... (truncated)");
          break;
        }
      }
      String res = out.toString();
      if (res.isEmpty()) {
        return "<empty>";
      }
      return res;
    } catch (Exception e) {
      return "<unreadable inputstream>";
    }
  }
}
