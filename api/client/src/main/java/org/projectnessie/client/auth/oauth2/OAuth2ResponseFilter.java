/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.client.auth.oauth2;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.ResponseFilter;
import org.projectnessie.client.http.Status;
import org.projectnessie.client.rest.io.CapturingInputStream;

class OAuth2ResponseFilter implements ResponseFilter {

  private final ObjectMapper objectMapper;

  OAuth2ResponseFilter(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public void filter(ResponseContext responseContext) {
    try {
      URI uri = responseContext.getRequestedUri();
      Status status = responseContext.getStatus();
      // The only normal responses are 200 OK and 302 Found (in the authorization code flow).
      if (status != Status.OK && status != Status.FOUND) {
        String body = null;
        IOException decodeError = null;
        if (responseContext.getInputStream() != null) {
          try (CapturingInputStream capturing =
              new CapturingInputStream(responseContext.getInputStream())) {
            if (responseContext.isJsonCompatibleResponse()) {
              try {
                ErrorResponse errorResponse =
                    objectMapper.readValue(capturing, ErrorResponse.class);
                String message = createJsonErrorMessage(uri, status, errorResponse);
                throw new OAuth2Exception(message, status, errorResponse);
              } catch (IOException e) {
                decodeError = e;
              }
            }
            body = capturing.capture();
          }
        }
        String message = createGenericErrorMessage(uri, status, body);
        HttpClientResponseException exception = new HttpClientResponseException(message, status);
        if (decodeError != null) {
          exception.addSuppressed(decodeError);
        }
        throw exception;
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new HttpClientException(e);
    }
  }

  private static String createJsonErrorMessage(
      URI uri, Status status, ErrorResponse errorResponse) {
    StringBuilder builder =
        new StringBuilder()
            .append("Request to ")
            .append(uri)
            .append(" failed with HTTP/")
            .append(status.getCode())
            .append(" and error code \"")
            .append(errorResponse.getErrorCode())
            .append("\"");
    if (errorResponse.getErrorDescription() != null) {
      builder.append(": ").append(errorResponse.getErrorDescription());
    }
    return builder.toString();
  }

  private static String createGenericErrorMessage(URI uri, Status status, String body) {
    StringBuilder builder =
        new StringBuilder()
            .append("Request to ")
            .append(uri)
            .append(" failed with HTTP/")
            .append(status.getCode());
    if (body != null && !body.isEmpty()) {
      builder.append(" and unparseable response body: ").append(body);
    } else {
      builder.append(" (response body was empty)");
    }
    return builder.toString();
  }
}
