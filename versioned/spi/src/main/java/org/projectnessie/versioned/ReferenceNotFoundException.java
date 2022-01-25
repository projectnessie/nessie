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
package org.projectnessie.versioned;

/** Exception thrown when a reference is not present in the store. */
public class ReferenceNotFoundException extends VersionStoreException {
  private static final long serialVersionUID = -4231207387427624751L;

  public ReferenceNotFoundException(String message) {
    super(message);
  }

  public ReferenceNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
