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
  UNKNOWN(500, null),
  REFERENCE_NOT_FOUND(404, NessieReferenceNotFoundException::new),
  REFERENCE_ALREADY_EXISTS(409, NessieReferenceAlreadyExistsException::new),
  CONTENT_NOT_FOUND(404, NessieContentNotFoundException::new),
  REFERENCE_CONFLICT(409, NessieReferenceConflictException::new),
  REFLOG_NOT_FOUND(404, NessieRefLogNotFoundException::new),
  BAD_REQUEST(400, NessieBadRequestException::new),
  FORBIDDEN(403, NessieForbiddenException::new),
  TOO_MANY_REQUESTS(429, NessieBackendThrottledException::new),
  NAMESPACE_NOT_FOUND(404, NessieNamespaceNotFoundException::new),
  NAMESPACE_ALREADY_EXISTS(409, NessieNamespaceAlreadyExistsException::new),
  NAMESPACE_NOT_EMPTY(409, NessieNamespaceNotEmptyException::new),
  ;

  private final int httpStatus;

  @SuppressWarnings("ImmutableEnumChecker")
  private final Function<NessieError, ? extends Exception> exceptionBuilder;

  <T extends Exception & ErrorCodeAware> ErrorCode(
      int status, Function<NessieError, T> exceptionBuilder) {
    this.httpStatus = status;
    this.exceptionBuilder = exceptionBuilder;
  }

  public int httpStatus() {
    return httpStatus;
  }

  public static Optional<Exception> asException(NessieError error) {
    return Optional.ofNullable(error.getErrorCode())
        .flatMap(e -> Optional.ofNullable(e.exceptionBuilder))
        .map(b -> b.apply(error));
  }
}
