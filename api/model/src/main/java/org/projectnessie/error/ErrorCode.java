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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Locale;
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
  SERVICE_UNAVAILABLE(503, NessieUnavailableException::new),
  REFERENCE_NOT_FOUND(404, NessieReferenceNotFoundException::new),
  REFERENCE_ALREADY_EXISTS(409, NessieReferenceAlreadyExistsException::new),
  CONTENT_NOT_FOUND(404, NessieContentNotFoundException::new),
  REFERENCE_CONFLICT(409, NessieReferenceConflictException::new),
  BAD_REQUEST(400, NessieBadRequestException::new),
  FORBIDDEN(403, NessieForbiddenException::new),
  TOO_MANY_REQUESTS(429, NessieBackendThrottledException::new),
  NAMESPACE_NOT_FOUND(404, NessieNamespaceNotFoundException::new),
  NAMESPACE_ALREADY_EXISTS(409, NessieNamespaceAlreadyExistsException::new),
  NAMESPACE_NOT_EMPTY(409, NessieNamespaceNotEmptyException::new),
  UNSUPPORTED_MEDIA_TYPE(415, NessieUnsupportedMediaTypeException::new),
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

  public static ErrorCode parse(String errorCode) {
    try {
      if (errorCode != null) {
        return ErrorCode.valueOf(errorCode.toUpperCase(Locale.ROOT));
      }
      return null;
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  public static final class Deserializer extends JsonDeserializer<ErrorCode> {
    @Override
    public ErrorCode deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String name = p.readValueAs(String.class);
      return name != null ? ErrorCode.parse(name) : null;
    }
  }
}
