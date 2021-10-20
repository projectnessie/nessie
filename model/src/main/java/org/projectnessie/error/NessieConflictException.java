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

/**
 * Base class for all exceptions that lead to the HTTP {@code Conflict} status code (409).
 *
 * <p>This exception should not be instantiated directly by server-side code.
 */
public class NessieConflictException extends BaseNessieClientServerException {

  public NessieConflictException(String message, Throwable cause) {
    super(message, 409, "Conflict", cause);
  }

  public NessieConflictException(String message) {
    super(message, 409, "Conflict");
  }

  public NessieConflictException(NessieError error) {
    super(error);
  }
}
