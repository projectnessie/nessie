/*
 * Copyright (C) 2023 Dremio
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

import java.net.URI;
import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.Status;

/** An exception thrown when the server replies with an OAuth2 error. */
public class OAuth2Exception extends HttpClientResponseException {

  private final String errorCode;

  OAuth2Exception(URI uri, Status status, ErrorResponse decodedBody, String rawBody) {
    super(createMessage(uri, status, decodedBody), uri, status, rawBody);
    this.errorCode = decodedBody.getErrorCode();
  }

  private static String createMessage(URI uri, Status status, ErrorResponse errorResponse) {
    StringBuilder builder =
        new StringBuilder()
            .append("Server replied to ")
            .append(uri)
            .append(" with HTTP status code ")
            .append(status.getCode())
            .append(" and error code \"")
            .append(errorResponse.getErrorCode())
            .append("\"");
    if (errorResponse.getErrorDescription() != null) {
      builder.append(": ").append(errorResponse.getErrorDescription());
    }
    return builder.toString();
  }

  public String getErrorCode() {
    return errorCode;
  }
}
