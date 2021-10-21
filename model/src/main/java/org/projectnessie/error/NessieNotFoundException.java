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
 * Base class for all exceptions that are represented by the HTTP {@code Not Found} status code
 * (404).
 *
 * <p>This exception should not be instantiated directly on the server-side. It may be instantiated
 * and thrown on the client side to represent cases when the server responded with the HTTP {@code
 * Not Found} status code, but no fine-grained error information was available.
 */
public class NessieNotFoundException extends BaseNessieClientServerException {

  public NessieNotFoundException(String message, Throwable cause) {
    super(message, 404, "Not Found", cause);
  }

  public NessieNotFoundException(String message) {
    super(message, 404, "Not Found");
  }

  public NessieNotFoundException(NessieError error) {
    super(error);
  }
}
