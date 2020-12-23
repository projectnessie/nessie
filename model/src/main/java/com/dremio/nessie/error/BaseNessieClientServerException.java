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

import java.io.IOException;

import javax.ws.rs.core.Response.Status;

/**
 * A caught exception that is thrown on the server and caught in the client.
 */
public class BaseNessieClientServerException extends IOException {

  private final Status status;
  private final String serverStackTrace;

  /**
   * Create an exception.
   * @param message Message
   * @param status HTTP status
   * @param cause The underlying cause.
   */
  public BaseNessieClientServerException(String message, Status status, Throwable cause) {
    super(message, cause);
    this.status = status;
    this.serverStackTrace = null;
  }

  /**
   * Create an exception.
   * @param message Message
   * @param status HTTP status
   */
  public BaseNessieClientServerException(String message, Status status) {
    super(message);
    this.status = status;
    this.serverStackTrace = null;
  }

  /**
   * Create an exception.
   * @param error The deserialized error object from the server.
   */
  public BaseNessieClientServerException(NessieError error) {
    super(error.getMessage());
    this.status = error.getStatus();
    this.serverStackTrace = error.getException();
  }

  public Status getStatus() {
    return status;
  }

  public String getReason() {
    return status.getReasonPhrase();
  }

  public String getServerStackTrace() {
    return serverStackTrace;
  }

}
