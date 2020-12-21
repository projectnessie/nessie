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
package com.dremio.nessie.client.http;

import java.util.Arrays;

/**
 * HTTP request status enum. Map return code to concrete status type with message.
 */
public enum Status {

  OK(200, "OK"),
  CREATED(201, "Created"),
  ACCEPTED(202, "Accepted"),
  NO_CONTENT(204, "No Content"),
  BAD_REQUEST(400, "Bad Request"),
  UNAUTHORIZED(401, "Unauthorized"),
  FORBIDDEN(403, "Forbidden"),
  NOT_FOUND(404, "Not Found"),
  METHOD_NOT_ALLOWED(405, "Method Not Allowed"),
  CONFLICT(409, "Conflict"),
  PRECONDITION_FAILED(412, "Precondition Failed"),
  UNSUPPORTED_MEDIA_TYPE(415, "Unsupported Media Type"),
  INTERNAL_SERVER_ERROR(500, "Internal Server Error");

  private final int code;
  private final String reason;


  Status(final int statusCode, final String reason) {
    this.code = statusCode;
    this.reason = reason;
  }

  /**
   * get Status enum from http return code.
   * @param code return code
   * @return Status for return code
   * @throws UnsupportedOperationException if unknown status code
   */
  public static Status fromCode(int code) {
    return Arrays.stream(Status.values())
                 .filter(x -> x.code == code)
                 .findFirst()
                 .orElseThrow(() -> new UnsupportedOperationException(String.format("Unknown status code %d", code)));
  }

  public int getCode() {
    return code;
  }

  public String getReason() {
    return reason;
  }
}
