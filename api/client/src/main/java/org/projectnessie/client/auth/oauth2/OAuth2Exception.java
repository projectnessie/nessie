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

import org.projectnessie.client.http.HttpClientResponseException;
import org.projectnessie.client.http.Status;

public class OAuth2Exception extends HttpClientResponseException {

  private final String errorCode;

  OAuth2Exception(Status status, ErrorResponse errorResponse) {
    super(createMessage(status, errorResponse), status);
    this.errorCode = errorResponse.getErrorCode();
  }

  private static String createMessage(Status status, ErrorResponse errorResponse) {
    StringBuilder builder =
        new StringBuilder()
            .append("OAuth2 server replied with HTTP status code ")
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
