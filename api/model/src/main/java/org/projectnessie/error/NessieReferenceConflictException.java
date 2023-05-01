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
 * This exception is thrown when the expected state of a reference (e.g. the hash of the HEAD of a
 * branch) does not match the hash provided by the caller or when the requested operation could not
 * be completed within the configured time and number of retries due to concurrent operations.
 */
public class NessieReferenceConflictException extends NessieConflictException {
  private final ReferenceConflicts referenceConflicts;

  public NessieReferenceConflictException(
      ReferenceConflicts referenceConflicts, String message, Throwable cause) {
    super(message, cause);
    this.referenceConflicts = referenceConflicts;
  }

  public NessieReferenceConflictException(String message, Throwable cause) {
    this(null, message, cause);
  }

  public NessieReferenceConflictException(String message) {
    super(message);
    this.referenceConflicts = null;
  }

  public NessieReferenceConflictException(NessieError error) {
    super(error);
    this.referenceConflicts = error.getErrorDetailsAsOrNull(ReferenceConflicts.class);
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.REFERENCE_CONFLICT;
  }

  @Override
  public ReferenceConflicts getErrorDetails() {
    return referenceConflicts;
  }
}
