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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * Represents Nessie-specific API error details.
 *
 * <p>All clients receive all properties that are not marked with an {@code @since} Javadoc tag.
 * Those properties, like the {@link #getErrorDetails()} property, will only be returned to clients,
 * if they sent the appropriate {@code Nessie-Client-Spec} header. The header value is an integer
 * that must be greater than or equal to the version noted in the {@code @since} Javadoc tag.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableNessieError.class)
@JsonDeserialize(as = ImmutableNessieError.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface NessieError {

  /** HTTP status code of this error. */
  int getStatus();

  /** Reason phrase for the HTTP status code. */
  String getReason();

  /** Nessie-specific Error message. */
  @Value.Default
  default String getMessage() {
    return getReason();
  }

  /** Nessie-specific error code. */
  @Value.Default
  @JsonDeserialize(using = ErrorCode.Deserializer.class)
  default ErrorCode getErrorCode() {
    return ErrorCode.UNKNOWN;
  }

  /**
   * Details for this particular error.
   *
   * @since Nessie Client spec 2, only serialized to clients, if clients send the HTTP header {@code
   *     Nessie-Client-Spec} with an integer value that is at least 2.
   */
  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(Include.NON_NULL)
  NessieErrorDetails getErrorDetails();

  default <T extends NessieErrorDetails> T getErrorDetailsAsOrNull(Class<T> type) {
    NessieErrorDetails details = getErrorDetails();
    return details != null && type.isAssignableFrom(details.getClass()) ? type.cast(details) : null;
  }

  /** Server-side exception stack trace related to this error (if available). */
  @Nullable
  @jakarta.annotation.Nullable
  String getServerStackTrace();

  /**
   * Client-side {@link Exception} related to the processing of the error response.
   *
   * <p>This {@link Exception} generally represents any errors related to the decoding of the
   * payload returned by the server to describe the error.
   *
   * @deprecated Use {@link #getClientProcessingError()}
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @JsonIgnore
  @Value.Auxiliary
  @Nullable
  @jakarta.annotation.Nullable
  @Deprecated
  Exception getClientProcessingException();

  /**
   * Client-side error message related to the processing of the error response.
   *
   * <p>This generally represents any errors related to the decoding of the payload returned by the
   * server to describe the error.
   */
  @JsonIgnore
  @Value.Auxiliary
  @Nullable
  @jakarta.annotation.Nullable
  String getClientProcessingError();

  /**
   * The full error message, including HTTP status, reason, server- and client-side exception stack
   * traces.
   */
  @JsonIgnore
  default String getFullMessage() {
    StringBuilder sb = new StringBuilder();
    sb.append(getReason()).append(" (HTTP/").append(getStatus()).append(')');
    sb.append(": ");
    String serverMessage = getMessage();
    if (serverMessage == null) {
      serverMessage = "[no error message found in HTTP response]";
    }
    sb.append(serverMessage);

    if (getServerStackTrace() != null) {
      sb.append("\n").append(getServerStackTrace());
    }

    if (getClientProcessingError() != null) {
      sb.append(
              "\nAdditionally, the client-side error below was caught while decoding the HTTP response: ")
          .append(getClientProcessingError());
    } else if (getClientProcessingException() != null) {
      sb.append(
              "\nAdditionally, the client-side exception below was caught while decoding the HTTP response: ")
          .append(getClientProcessingException());
    }

    return sb.toString();
  }
}
