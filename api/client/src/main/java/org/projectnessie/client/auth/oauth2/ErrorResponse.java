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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Error response as declared in <a
 * href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.2">Section 5.2</a>.
 *
 * <p>Example of response for the client credentials flow:
 *
 * <pre>
 * HTTP/1.1 400 Bad Request
 * Content-Type: application/json;charset=UTF-8
 * Cache-Control: no-store
 * Pragma: no-cache
 *
 * {
 *   "error":"invalid_request"
 * }
 * </pre>
 */
@Value.Immutable
@JsonSerialize(as = ImmutableErrorResponse.class)
@JsonDeserialize(as = ImmutableErrorResponse.class)
interface ErrorResponse {

  /**
   * REQUIRED. A single ASCII [USASCII] error code. Values for the "error" parameter MUST NOT
   * include characters outside the set %x20-21 / %x23-5B / %x5D-7E.
   *
   * <p>Error codes are defined in <a
   * href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.2">RFC 6749, Section 5.2</a>, but
   * servers tend to define their own error codes.
   */
  @JsonProperty("error")
  String getErrorCode();

  /**
   * OPTIONAL. Human-readable ASCII [USASCII] text providing additional information, used to assist
   * the client developer in understanding the error that occurred. Values for the
   * "error_description" parameter MUST NOT include characters outside the set %x20-21 / %x23-5B /
   * %x5D-7E.
   */
  @JsonProperty("error_description")
  @Nullable
  String getErrorDescription();
}
