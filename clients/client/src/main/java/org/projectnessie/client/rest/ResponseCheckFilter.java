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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.Status;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieError;
import org.projectnessie.error.NessieNotFoundException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

public class ResponseCheckFilter {

  /**
   * check that response had a valid return code. Throw exception if not.
   * @param con open http connection
   * @param mapper Jackson ObjectMapper instance for this client
   * @throws IOException Throws IOException for certain error types.
   */
  public static void checkResponse(ResponseContext con, ObjectMapper mapper) throws IOException {
    final Status status;
    final NessieError error;
    // this could IOException, in which case the error will be passed up to the client as an HttpClientException
    status = con.getResponseCode();
    if (status.getCode() > 199 && status.getCode() < 300) {
      return;
    }

    // this could IOException, in which case the error will be passed up to the client as an HttpClientException
    try (InputStream is = con.getErrorStream()) {
      error = decodeErrorObject(status, is, mapper.readerFor(NessieError.class));
    }

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
      case INTERNAL_SERVER_ERROR:
        throw new NessieInternalServerException(error);
      default:
        throw new NessieServiceException(error);
    }

  }

  private static NessieError decodeErrorObject(Status status, InputStream inputStream, ObjectReader reader) {
    NessieError error;
    if (inputStream == null) {
      error = new NessieError(status.getCode(), status.getReason(),
                              "Could not parse error object in response.",
                              new RuntimeException("Could not parse error object in response."));
    } else {
      String text = null;
      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buf = new byte[512];
        while (true) {
          int rd = inputStream.read(buf);
          if (rd < 0) {
            break;
          }
          os.write(buf, 0, rd);
        }
        byte[] bytes = os.toByteArray();
        text = String.format("%s (HTTP/%d): %s", status.getReason(), status.getCode(), new String(bytes));

        error = reader.readValue(new ByteArrayInputStream(bytes));
      } catch (IOException e) {
        error = new NessieError(status.getCode(), text != null ? text : status.getReason(), null, e);
      }
    }
    return error;
  }

}
