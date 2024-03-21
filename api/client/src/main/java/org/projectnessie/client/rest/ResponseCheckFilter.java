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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
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
    final Status status;
    final NessieError error;
    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    status = con.getResponseCode();
    if (status.getCode() > 199 && status.getCode() < 300) {
      return;
    }

    // this could IOException, in which case the error will be passed up to the client as an
    // HttpClientException
    try (InputStream is = con.getErrorStream()) {
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

    if (error.getClientProcessingException() != null) {
      exception.addSuppressed(error.getClientProcessingException()); // for trouble-shooting
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
              .clientProcessingException(
                  new RuntimeException("Could not parse error object in response."))
              .build();
    } else {
      CapturingInputStream capturing = new CapturingInputStream(inputStream);
      try {
        JsonNode errorData = reader.readTree(capturing);
        try {
          error = reader.treeToValue(errorData, NessieError.class);
        } catch (JsonProcessingException e) {

          // If the error payload is valid JSON, but does not represent a NessieError, it is likely
          // produced by Quarkus and contains the server-side logged error ID. Report the raw JSON
          // text to the caller for trouble-shooting.
          error =
              ImmutableNessieError.builder()
                  .message(errorData.toString())
                  .status(status.getCode())
                  .reason(status.getReason())
                  .clientProcessingException(e)
                  .build();
        }
      } catch (IOException e) {
        error =
            ImmutableNessieError.builder()
                .message(
                    "Could not parse error object in response beginning with: "
                        + capturing.captured())
                .status(status.getCode())
                .reason(status.getReason())
                .clientProcessingException(e)
                .build();
      }
    }
    return error;
  }

  /**
   * Captures the first 2kB of the input in case the response is not parsable, to provide a better
   * error message to the user.
   */
  static final class CapturingInputStream extends FilterInputStream {
    static final int CAPTURE_LEN = 2048;
    private final byte[] capture = new byte[CAPTURE_LEN];
    private int captured;

    CapturingInputStream(InputStream delegate) {
      super(delegate);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int rd = super.read(b, off, len);
      int captureRemain = capture.length - captured;
      if (rd > 0 && captureRemain > 0) {
        int copy = Math.min(rd, captureRemain);
        System.arraycopy(b, off, capture, captured, copy);
        captured += copy;
      }
      return rd;
    }

    @Override
    public int read() throws IOException {
      int rd = super.read();
      if (rd >= 0 && captured < capture.length) {
        capture[captured++] = (byte) rd;
      }
      return rd;
    }

    public String captured() {
      return new String(capture, 0, captured, UTF_8);
    }
  }
}
