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

import java.util.Optional;
import java.util.function.Function;

/**
 * Defines Nessie error codes that are more fine-grained than HTTP status codes and maps them to
 * exception classes.
 *
 * <p>The enum names also designate error code in the JSON representation of {@link NessieError}.
 */
public enum ErrorCode {
  UNKNOWN(null),
  REFERENCE_NOT_FOUND(NessieReferenceNotFoundException::new),
  REFERENCE_ALREADY_EXISTS(NessieReferenceAlreadyExistsException::new),
  CONTENTS_NOT_FOUND(NessieContentsNotFoundException::new),
  REFERENCE_CONFLICT(NessieReferenceConflictException::new),
  ;

  private final Function<NessieError, ? extends BaseNessieClientServerException> exceptionBuilder;

  <T extends BaseNessieClientServerException> ErrorCode(Function<NessieError, T> exceptionBuilder) {
    this.exceptionBuilder = exceptionBuilder;
  }

  public static Optional<BaseNessieClientServerException> asException(NessieError error) {
    return Optional.ofNullable(error.getErrorCode())
        .flatMap(e -> Optional.ofNullable(e.exceptionBuilder))
        .map(b -> b.apply(error));
  }
}
