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

/** This exception indicates that an argument to a Nessie API method had an invalid value. */
public class NessieIllegalArgumentException extends BaseNessieClientServerException {
  public NessieIllegalArgumentException(String message, String reason, Throwable cause) {
    super(message, 400, reason, cause);
  }

  public NessieIllegalArgumentException(String message, String reason) {
    super(message, 400, reason);
  }

  public NessieIllegalArgumentException(String message) {
    super(message, 400, "Illegal Argument");
  }

  public NessieIllegalArgumentException(NessieError error) {
    super(error);
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.ILLEGAL_ARGUMENT;
  }
}
