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

import static org.projectnessie.client.http.impl.HttpUtils.checkArgument;

import java.util.Arrays;

/** HTTP request status enum. Map return code to concrete status type with message. */
public final class Status {

  private static final Status[][] LINES = new Status[10][];

  static {
    Arrays.fill(LINES, new Status[0]);
  }

  public static final int OK_CODE = 200;
  public static final Status OK = addCode(OK_CODE, "OK");
  public static final int CREATED_CODE = 201;
  public static final Status CREATED = addCode(CREATED_CODE, "Created");
  public static final int ACCEPTED_CODE = 202;
  public static final Status ACCEPTED = addCode(ACCEPTED_CODE, "Accepted");
  public static final int NO_CONTENT_CODE = 204;
  public static final Status NO_CONTENT = addCode(NO_CONTENT_CODE, "No Content");
  public static final int RESET_CONTENT_CODE = 205;
  public static final Status RESET_CONTENT = addCode(RESET_CONTENT_CODE, "Reset Content");
  public static final int PARTIAL_CONTENT_CODE = 206;
  public static final Status PARTIAL_CONTENT = addCode(PARTIAL_CONTENT_CODE, "Partial Content");
  public static final int MOVED_PERMANENTLY_CODE = 301;
  public static final Status MOVED_PERMANENTLY =
      addCode(MOVED_PERMANENTLY_CODE, "Moved Permanently");
  public static final int FOUND_CODE = 302;
  public static final Status FOUND = addCode(FOUND_CODE, "Found");
  public static final int SEE_OTHER_CODE = 303;
  public static final Status SEE_OTHER = addCode(SEE_OTHER_CODE, "See Other");
  public static final int NOT_MODIFIED_CODE = 304;
  public static final Status NOT_MODIFIED = addCode(NOT_MODIFIED_CODE, "Not Modified");
  public static final int USE_PROXY_CODE = 305;
  public static final Status USE_PROXY = addCode(USE_PROXY_CODE, "Use Proxy");
  public static final int TEMPORARY_REDIRECT_CODE = 307;
  public static final Status TEMPORARY_REDIRECT =
      addCode(TEMPORARY_REDIRECT_CODE, "Temporary Redirect");
  public static final int BAD_REQUEST_CODE = 400;
  public static final Status BAD_REQUEST = addCode(BAD_REQUEST_CODE, "Bad Request");
  public static final int UNAUTHORIZED_CODE = 401;
  public static final Status UNAUTHORIZED = addCode(UNAUTHORIZED_CODE, "Unauthorized");
  public static final int PAYMENT_REQUIRED_CODE = 402;
  public static final Status PAYMENT_REQUIRED = addCode(PAYMENT_REQUIRED_CODE, "Payment Required");
  public static final int FORBIDDEN_CODE = 403;
  public static final Status FORBIDDEN = addCode(FORBIDDEN_CODE, "Forbidden");
  public static final int NOT_FOUND_CODE = 404;
  public static final Status NOT_FOUND = addCode(NOT_FOUND_CODE, "Not Found");
  public static final int METHOD_NOT_ALLOWED_CODE = 405;
  public static final Status METHOD_NOT_ALLOWED =
      addCode(METHOD_NOT_ALLOWED_CODE, "Method Not Allowed");
  public static final int NOT_ACCEPTABLE_CODE = 406;
  public static final Status NOT_ACCEPTABLE = addCode(NOT_ACCEPTABLE_CODE, "Not Acceptable");
  public static final int PROXY_AUTHENTICATION_REQUIRED_CODE = 407;
  public static final Status PROXY_AUTHENTICATION_REQUIRED =
      addCode(PROXY_AUTHENTICATION_REQUIRED_CODE, "Proxy Authentication Required");
  public static final int REQUEST_TIMEOUT_CODE = 408;
  public static final Status REQUEST_TIMEOUT = addCode(REQUEST_TIMEOUT_CODE, "Request Timeout");
  public static final int CONFLICT_CODE = 409;
  public static final Status CONFLICT = addCode(CONFLICT_CODE, "Conflict");
  public static final int GONE_CODE = 410;
  public static final Status GONE = addCode(GONE_CODE, "Gone");
  public static final int LENGTH_REQUIRED_CODE = 411;
  public static final Status LENGTH_REQUIRED = addCode(LENGTH_REQUIRED_CODE, "Length Required");
  public static final int PRECONDITION_FAILED_CODE = 412;
  public static final Status PRECONDITION_FAILED =
      addCode(PRECONDITION_FAILED_CODE, "Precondition Failed");
  public static final int REQUEST_ENTITY_TOO_LARGE_CODE = 413;
  public static final Status REQUEST_ENTITY_TOO_LARGE =
      addCode(REQUEST_ENTITY_TOO_LARGE_CODE, "Request Entity Too Large");
  public static final int REQUEST_URI_TOO_LONG_CODE = 414;
  public static final Status REQUEST_URI_TOO_LONG =
      addCode(REQUEST_URI_TOO_LONG_CODE, "Request-URI Too Long");
  public static final int UNSUPPORTED_MEDIA_TYPE_CODE = 415;
  public static final Status UNSUPPORTED_MEDIA_TYPE =
      addCode(UNSUPPORTED_MEDIA_TYPE_CODE, "Unsupported Media Type");
  public static final int REQUESTED_RANGE_NOT_SATISFIABLE_CODE = 416;
  public static final Status REQUESTED_RANGE_NOT_SATISFIABLE =
      addCode(REQUESTED_RANGE_NOT_SATISFIABLE_CODE, "Requested Range Not Satisfiable");
  public static final int EXPECTATION_FAILED_CODE = 417;
  public static final Status EXPECTATION_FAILED =
      addCode(EXPECTATION_FAILED_CODE, "Expectation Failed");
  public static final int PRECONDITION_REQUIRED_CODE = 428;
  public static final Status PRECONDITION_REQUIRED =
      addCode(PRECONDITION_REQUIRED_CODE, "Precondition Required");
  public static final int TOO_MANY_REQUESTS_CODE = 429;
  public static final Status TOO_MANY_REQUESTS =
      addCode(TOO_MANY_REQUESTS_CODE, "Too Many Requests");
  public static final int REQUEST_HEADER_FIELDS_TOO_LARGE_CODE = 431;
  public static final Status REQUEST_HEADER_FIELDS_TOO_LARGE =
      addCode(REQUEST_HEADER_FIELDS_TOO_LARGE_CODE, "Request Header Fields Too Large");
  public static final int INTERNAL_SERVER_ERROR_CODE = 500;
  public static final Status INTERNAL_SERVER_ERROR =
      addCode(INTERNAL_SERVER_ERROR_CODE, "Internal Server Error");
  public static final int NOT_IMPLEMENTED_CODE = 501;
  public static final Status NOT_IMPLEMENTED = addCode(NOT_IMPLEMENTED_CODE, "Not Implemented");
  public static final int BAD_GATEWAY_CODE = 502;
  public static final Status BAD_GATEWAY = addCode(BAD_GATEWAY_CODE, "Bad Gateway");
  public static final int SERVICE_UNAVAILABLE_CODE = 503;
  public static final Status SERVICE_UNAVAILABLE =
      addCode(SERVICE_UNAVAILABLE_CODE, "Service Unavailable");
  public static final int GATEWAY_TIMEOUT_CODE = 504;
  public static final Status GATEWAY_TIMEOUT = addCode(GATEWAY_TIMEOUT_CODE, "Gateway Timeout");
  public static final int HTTP_VERSION_NOT_SUPPORTED_CODE = 505;
  public static final Status HTTP_VERSION_NOT_SUPPORTED =
      addCode(HTTP_VERSION_NOT_SUPPORTED_CODE, "HTTP Version Not Supported");
  public static final int NETWORK_AUTHENTICATION_REQUIRED_CODE = 511;
  public static final Status NETWORK_AUTHENTICATION_REQUIRED =
      addCode(NETWORK_AUTHENTICATION_REQUIRED_CODE, "Network Authentication Required");

  private final int code;
  private final String reason;

  private static Status addCode(int code, String reason) {
    Status[][] lines = LINES;
    int line = code / 100;
    int code100 = code % 100;
    Status[] l = lines[line];
    if (l.length < code100 + 1) {
      l = lines[line] = Arrays.copyOf(l, code100 + 1);
    }
    return l[code100] = new Status(code, reason);
  }

  private Status(int statusCode, String reason) {
    this.code = statusCode;
    this.reason = reason;
  }

  /**
   * get Status enum from http return code.
   *
   * @param code return code
   * @return Status for return code
   * @throws UnsupportedOperationException if unknown status code
   */
  public static Status fromCode(int code) {
    checkArgument(code >= 0 && code < 1000, "Illegal HTTP status code %d", code);
    int code100 = code % 100;
    Status[] l = LINES[code / 100];
    if (l.length > code100) {
      Status s = l[code100];
      if (s != null) {
        return s;
      }
    }
    return new Status(code, "");
  }

  public int getCode() {
    return code;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Status status = (Status) o;
    return code == status.code && reason.equals(status.reason);
  }

  @Override
  public int hashCode() {
    int result = code;
    result = 31 * result + reason.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return code + "/" + reason;
  }
}
