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

/**
 * Exception thrown when the hash associated with a named reference does not match with the hash
 * provided by the caller.
 */
public class ReferenceConflictException extends VersionStoreException {
  private static final long serialVersionUID = 4381980193289615523L;

  public ReferenceConflictException(String message) {
    super(message);
  }

  public ReferenceConflictException(String message, Throwable t) {
    super(message, t);
  }
}
