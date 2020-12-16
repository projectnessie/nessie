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
package com.dremio.nessie.error;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NessieError {

  private final String message;
  private final int status;
  private final String serverStackTrace;
  private final Exception clientProcessingException;

  @JsonCreator
  public NessieError(
      @JsonProperty("message") String message,
      @JsonProperty("status") int status,
      @JsonProperty("serverStackTrace") String serverStackTrace) {
    this(message, status, serverStackTrace, null);
  }

  /**
   * Create Error.
   *
   * @param message             Message of error.
   * @param status              Status of error.
   * @param serverStackTrace    Server stack trace, if available.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(String message, int status, String serverStackTrace, Exception processingException) {
    this.message = message;
    this.status = status;
    this.serverStackTrace = serverStackTrace;
    this.clientProcessingException = processingException;
  }

  /**
   * Create Error.
   *
   * @param statusCode          Status of error.
   * @param serverStackTrace    Server stack trace, if available.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(int statusCode, String serverStackTrace, Exception processingException) {
    this.status = statusCode;
    this.message = standardMessage(status);
    this.serverStackTrace = serverStackTrace;
    this.clientProcessingException = processingException;
  }

  private String standardMessage(int status) {
    switch (status) {
      case 200:
        return "OK";
      case 201:
        return "Created";
      case 204:
        return "No Content";
      case 400:
        return "Bad Request";
      case 401:
        return "Unauthorized";
      case 403:
        return "Forbidden";
      case 404:
        return "Not Found";
      case 405:
        return "Method Not Allowed";
      case 409:
        return "Conflict";
      case 412:
        return "Precondition Failed";
      case 500:
        return "Internal Server Error";
      default:
        throw new UnsupportedOperationException(String.format("Cannot identify given error type %d", status));
    }
  }


  public String getMessage() {
    return message;
  }

  public int getStatus() {
    return status;
  }

  public String getServerStackTrace() {
    return serverStackTrace;
  }

  @JsonIgnore
  public Exception getClientProcessingException() {
    return clientProcessingException;
  }

  /**
   * Get full error message.
   *
   * @return Full error message.
   */
  @JsonIgnore
  public String getFullMessage() {
    if (serverStackTrace != null) {
      return String.format("%s\nStatus Code: %d\nServer Stack Trace:\n%s", message,
                           status, serverStackTrace);
    }

    if (clientProcessingException != null) {
      StringWriter sw = new StringWriter();
      clientProcessingException.printStackTrace(new PrintWriter(sw));
      return String.format("%s\nStatus Code: %d\nClient Processing Failure:\n%s", message,
                           status, sw.toString());
    }
    return message;
  }
}
