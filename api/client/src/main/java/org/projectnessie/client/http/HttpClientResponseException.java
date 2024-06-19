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

/**
 * A generic exception thrown when the server replies with an unexpected status code.
 *
 * <p>This exception is generally thrown when the status code is not in the 2xx or 3xx range, and if
 * a response filter did not throw a more specific exception.
 */
public class HttpClientResponseException extends HttpClientException {

  private final Status status;

  public HttpClientResponseException(String message, Status status) {
    super(message);
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }
}
