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

import java.util.Locale;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a constraint violation like a violation of an {@code @NonNull}.
 * The layout is similar to the one of {@code org.jboss.resteasy.api.validation.ResteasyConstraintViolation}.
 */
public class ConstraintViolation {

  public enum Type {
    CLASS, PROPERTY, PARAMETER, RETURN_VALUE
  }

  private final Type type;
  private final String path;
  private final String message;
  private final String value;

  /**
   * Constructs the constraint violation.
   * @param type violation type
   * @param path violation path
   * @param message violation message
   * @param value violating value
   */
  @JsonCreator
  public ConstraintViolation(
      @JsonProperty("constraintType") Type type,
      @JsonProperty("path") String path,
      @JsonProperty("message") String message,
      @JsonProperty("value") String value) {
    this.type = type;
    this.path = path;
    this.message = message;
    this.value = value;
  }

  public Type getType() {
    return type;
  }

  public String getPath() {
    return path;
  }

  public String getMessage() {
    return message;
  }

  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return (type != null ? type.name().toLowerCase(Locale.ROOT) : "")
           + ((path != null && !path.isEmpty()) ? (" " + path) : "")
           + ((message != null && !message.isEmpty()) ? (" " + message) : "")
           + (value != null && !value.isEmpty() ? " (value='" + value + "')" : "");
  }
}
