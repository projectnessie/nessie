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
package org.projectnessie.error;

import java.io.IOException;

/** A caught exception that is thrown on the server and caught in the client. */
public class BaseNessieClientServerException extends IOException implements ErrorCodeAware {

  private final String serverStackTrace;

  /**
   * Server-side constructor.
   *
   * @param message Message
   * @param cause The underlying cause.
   */
  public BaseNessieClientServerException(String message, Throwable cause) {
    super(message, cause);
    this.serverStackTrace = null;
  }

  /**
   * Server-side constructor.
   *
   * @param message Message
   */
  public BaseNessieClientServerException(String message) {
    super(message);
    this.serverStackTrace = null;
  }

  /**
   * Client-side constructor.
   *
   * @param error The deserialized error object from the server.
   */
  public BaseNessieClientServerException(NessieError error) {
    super(error.getMessage());
    this.serverStackTrace = error.getServerStackTrace();
  }

  public int getStatus() {
    return getErrorCode().httpStatus();
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.UNKNOWN;
  }

  public String getServerStackTrace() {
    return serverStackTrace;
  }
}
