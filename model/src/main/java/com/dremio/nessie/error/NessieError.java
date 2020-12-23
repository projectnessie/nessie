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
package com.dremio.nessie.error;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NessieError {

  private final String message;
  private final Status status;
  private final String exception;
  private final Exception clientProcessingException;

  /**
   * Deserialize error message from the server.
   * <p>
   * There are two types of JSON objects that this constructor can handle:
   * </p>
   * <ol>
   * <li>A "normal" error produced by Nessie itself, providing the parameters {@code message},
   * {@code status}, {@code exception} from {@link NessieError}</li>
   * <li>An "early" error produced by Resteasy as from {@code org.jboss.resteasy.api.validation.ViolationReport},
   * providing the parameters {@code message}, {@code propertyViolations}, {@code classViolations},
   * {@code parameterViolations}, {@code returnValueViolations}. The violations are not kept around.</li>
   * </ol>
   *
   * @param message Error message
   * @param status HTTP status code
   * @param exception exception, if present (can be empty or {@code null}
   * @param propertyViolations list of violations in properties
   * @param classViolations list of violations in classes
   * @param parameterViolations list of violations in parameters
   * @param returnValueViolations list of violations in return value
   */
  @JsonCreator
  public NessieError(
      @JsonProperty("message") String message,
      @JsonProperty("status") Status status,
      @JsonProperty("exception") String exception,
      @JsonProperty("propertyViolations") List<ConstraintViolation> propertyViolations,
      @JsonProperty("classViolations") List<ConstraintViolation> classViolations,
      @JsonProperty("parameterViolations") List<ConstraintViolation> parameterViolations,
      @JsonProperty("returnValueViolations") List<ConstraintViolation> returnValueViolations) {
    this(concatViolations(message, propertyViolations, classViolations, parameterViolations, returnValueViolations),
         status != null ? status : Status.BAD_REQUEST,
         exception,
         null);
  }

  /**
   * Appends the violations to the error message and returns the string representation.
   */
  private static String concatViolations(
      String message,
      List<ConstraintViolation> propertyViolations,
      List<ConstraintViolation> classViolations,
      List<ConstraintViolation> parameterViolations,
      List<ConstraintViolation> returnValueViolations) {

    // appends violations in their string representation to a message
    BiFunction<String, List<ConstraintViolation>, String> concat = (msg, l) ->
        l != null && !l.isEmpty()
        ? (msg != null ? msg + " / " : "") + l.stream().map(Object::toString).collect(Collectors.joining(" / "))
        : msg;

    message = concat.apply(message, propertyViolations);
    message = concat.apply(message, classViolations);
    message = concat.apply(message, parameterViolations);
    message = concat.apply(message, returnValueViolations);

    return message;
  }

  /**
   * Create Error.
   * @param message Message of error.
   * @param status Status of error.
   * @param exception Server stack trace, if available, can be {@code null}.
   * @param processingException Any processing exceptions that happened on the client.
   */
  public NessieError(String message, Status status, String exception, Exception processingException) {
    super();
    this.message = message;
    this.status = status;
    this.exception = exception;
    this.clientProcessingException = processingException;
  }

  /**
   * Create Error.
   * @param message Message of error.
   * @param status Status of error.
   * @param exception Server stack trace, if available, can be {@code null}.
   */
  public NessieError(String message, Status status, String exception) {
    this(message, status, exception, null);
  }

  public String getMessage() {
    return message;
  }

  public Status getStatus() {
    return status;
  }

  public String getException() {
    return exception;
  }

  @JsonIgnore
  public Exception getClientProcessingException() {
    return clientProcessingException;
  }

  /**
   * Get full error message.
   * @return Full error message.
   */
  @JsonIgnore
  public String getFullMessage() {
    StringBuilder sb = new StringBuilder();
    if (status != null) {
      sb.append(status.getReasonPhrase())
        .append(" (HTTP/").append(status.getStatusCode()).append(')');
    }

    if (message != null) {
      if (sb.length() > 0) {
        sb.append(": ");
      }
      sb.append(message);
    }

    if (exception != null) {
      sb.append("\n").append(exception);
    }
    if (clientProcessingException != null) {
      StringWriter sw = new StringWriter();
      clientProcessingException.printStackTrace(new PrintWriter(sw));
      sb.append("\n").append(sw);
    }
    return sb.toString();
  }
}
