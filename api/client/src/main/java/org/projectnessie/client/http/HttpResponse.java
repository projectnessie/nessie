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
package org.projectnessie.client.http;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.projectnessie.client.rest.NessieBadResponseException;
import org.projectnessie.client.rest.io.CapturingInputStream;
import org.projectnessie.error.ImmutableNessieError;
import org.projectnessie.error.NessieError;

/** Simple holder for http response object. */
public class HttpResponse {

  private final ResponseContext responseContext;
  private final ObjectMapper mapper;

  public HttpResponse(ResponseContext context, ObjectMapper mapper) {
    this.responseContext = context;
    this.mapper = mapper;
  }

  /**
   * Read the entity from the underlying HTTP response.
   *
   * @return the entity, or null if the response has no content
   * @throws HttpClientException if the entity cannot be read
   * @throws NessieBadResponseException if the entity is not JSON compatible
   */
  public <V> V readEntity(Class<V> clazz) {
    ObjectReader reader = mapper.readerFor(clazz);
    try {
      return decodeEntity(reader, responseContext.getInputStream());
    } catch (IOException e) {
      throw new HttpClientException("Failed to read entity", e);
    }
  }

  private <V> V decodeEntity(ObjectReader reader, InputStream is) throws IOException {
    if (is != null) {
      CapturingInputStream capturing = new CapturingInputStream(is);
      try {
        if (responseContext.isJsonCompatibleResponse()) {
          // readValues tolerates empty responses and returns an empty iterator in that case;
          // this allows to handle these cases gracefully
          MappingIterator<V> values = reader.readValues(capturing);
          if (values.hasNextValue()) {
            return values.nextValue();
          }
        } else {
          String captured = capturing.capture();
          if (!captured.isEmpty()) {
            throw unparseableResponse(captured, null);
          }
        }
      } catch (IOException e) {
        throw unparseableResponse(capturing.capture(), e);
      } finally {
        capturing.close();
      }
    }
    // In case the Nessie server returns no content (e.g. because it's on an older version)
    // but the client expects a response, just return null here and cross fingers that
    // the code doesn't expect a non-null value.
    return null;
  }

  private NessieBadResponseException unparseableResponse(String captured, Exception cause) {
    captured = captured.trim();
    Status status = responseContext.getStatus();
    String message =
        String.format(
            "Expected the server to return a JSON compatible response, "
                + "but the server replied with Content-Type '%s' from '%s' %s. "
                + "Check the Nessie REST API base URI. "
                + "Nessie REST API base URI usually ends in '/api/v1' or '/api/v2', "
                + "but your service provider may have a different URL pattern.",
            responseContext.getContentType(),
            responseContext.getRequestedUri(),
            captured.isEmpty()
                ? "and no response body"
                : "and a response body beginning with: \"" + captured + "\"");
    NessieError error =
        ImmutableNessieError.builder()
            .message(message)
            .status(status.getCode())
            .reason(status.getReason())
            .build();
    NessieBadResponseException exception = new NessieBadResponseException(error);
    if (cause != null) {
      exception.initCause(cause);
    }
    return exception;
  }

  public URI getRequestUri() {
    return responseContext.getRequestedUri();
  }

  public Status getStatus() {
    return responseContext.getStatus();
  }
}
