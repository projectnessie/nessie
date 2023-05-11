/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.immutables.value.Value;

/** Represents documentation for a content object in Nessie. */
@Value.Immutable
@JsonSerialize(as = ImmutableDocumentation.class)
@JsonDeserialize(as = ImmutableDocumentation.class)
public interface Documentation {
  static ImmutableDocumentation.Builder builder() {
    return ImmutableDocumentation.builder();
  }

  /** Mime type of the documentation. */
  @NotNull
  @jakarta.validation.constraints.NotNull
  @Value.Parameter(order = 1)
  String getMimeType();

  /** URI of the documentation. If present, {@link #getText()} must not be specified. */
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 1)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String getLocation();

  /** Documentation text. If present, {@link #getLocation()} must not be specified. */
  @Nullable
  @jakarta.annotation.Nullable
  @Value.Parameter(order = 1)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  String getText();

  @Value.Check
  default void check() {
    boolean locationNull = getLocation() == null;
    boolean textNull = getText() == null;
    if (locationNull == textNull) {
      throw new IllegalStateException(
          "Either 'location' or 'test' must be specified, but not both.");
    }
  }
}
