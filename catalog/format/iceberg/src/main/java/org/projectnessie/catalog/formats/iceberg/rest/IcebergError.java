/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergError.class)
@JsonDeserialize(as = ImmutableIcebergError.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergError {

  @Nullable
  String message();

  @Nullable
  String type();

  @Nullable
  Integer code();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<String> stack();

  static Builder builder() {
    return ImmutableIcebergError.builder();
  }

  static IcebergError icebergError(int code, String type, String message, List<String> stack) {
    return ImmutableIcebergError.of(message, type, code, stack);
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergError instance);

    @CanIgnoreReturnValue
    Builder code(Integer code);

    @CanIgnoreReturnValue
    Builder message(String message);

    @CanIgnoreReturnValue
    Builder type(String type);

    @CanIgnoreReturnValue
    Builder addStack(String element);

    @CanIgnoreReturnValue
    Builder addStack(String... elements);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder stack(Iterable<String> elements);

    @CanIgnoreReturnValue
    Builder addAllStack(Iterable<String> elements);

    IcebergError build();
  }
}
