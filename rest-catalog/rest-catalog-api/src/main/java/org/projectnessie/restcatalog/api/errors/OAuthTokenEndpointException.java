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
package org.projectnessie.restcatalog.api.errors;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when the OAuth token endpoint returns an error.
 *
 * <p>This endpoint is special because it is supposed to return error responses containing typical
 * OIDC error payloads, e.g.:
 *
 * <pre>
 * {
 *  "error": "invalid_grant",
 *  "error_description": "Invalid refresh token (expired)"
 * }
 * </pre>
 *
 * <p>Because of that, we need to handle it differently from other endpoints and refrain from
 * extending {@link GenericIcebergRestException}.
 */
public class OAuthTokenEndpointException extends RuntimeException {

  private final int code;
  private final String error;

  public OAuthTokenEndpointException(int code, String error, String errorDescription) {
    super(errorDescription);
    this.code = code;
    this.error = error;
  }

  public int getCode() {
    return code;
  }

  public String getError() {
    return error;
  }

  public Map<String, String> getDetails() {
    Map<String, String> r = new HashMap<>();
    r.put("error", error);
    r.put("error_description", getMessage());
    return unmodifiableMap(r);
  }
}
