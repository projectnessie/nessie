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
package org.projectnessie.client.http;

import java.net.URI;
import java.util.Optional;

/**
 * A generic exception thrown when the server replies with an unexpected status code.
 *
 * <p>This exception is generally thrown when the status code is not in the 2xx or 3xx range, and if
 * a response filter did not throw a more specific exception.
 */
public class HttpClientResponseException extends HttpClientException {

  private final URI uri;
  private final Status status;
  private final String body;

  public HttpClientResponseException(URI uri, Status status, String body) {
    this(createMessage(uri, status, body), uri, status, body);
  }

  public HttpClientResponseException(String message, URI uri, Status status, String body) {
    super(message);
    this.uri = uri;
    this.status = status;
    this.body = body;
  }

  public URI getUri() {
    return uri;
  }

  public Status getStatus() {
    return status;
  }

  public Optional<String> getBody() {
    return body == null || body.isEmpty() ? Optional.empty() : Optional.of(body);
  }

  private static String createMessage(URI uri, Status status, String body) {
    StringBuilder builder =
        new StringBuilder()
            .append("Server replied to ")
            .append(uri)
            .append(" with HTTP status code ")
            .append(status.getCode());
    if (body != null && !body.isEmpty()) {
      builder.append(" and response body:\n").append(body);
    } else {
      builder.append(" (response body was empty)");
    }
    return builder.toString();
  }
}
