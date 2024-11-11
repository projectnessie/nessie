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
import java.util.List;
import java.util.Map;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergConfigResponse.class)
@JsonDeserialize(as = ImmutableIcebergConfigResponse.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergConfigResponse {
  Map<String, String> defaults();

  Map<String, String> overrides();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<IcebergEndpoint> endpoints();

  static Builder builder() {
    return ImmutableIcebergConfigResponse.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {

    @CanIgnoreReturnValue
    Builder from(IcebergConfigResponse instance);

    @CanIgnoreReturnValue
    Builder putDefault(String key, String value);

    @CanIgnoreReturnValue
    Builder putDefault(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder defaults(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllDefaults(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putOverride(String key, String value);

    @CanIgnoreReturnValue
    Builder putOverride(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder overrides(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllOverrides(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder addEndpoint(IcebergEndpoint element);

    @CanIgnoreReturnValue
    Builder addEndpoints(IcebergEndpoint... elements);

    @CanIgnoreReturnValue
    Builder endpoints(Iterable<? extends IcebergEndpoint> elements);

    @CanIgnoreReturnValue
    Builder addAllEndpoints(Iterable<? extends IcebergEndpoint> elements);

    IcebergConfigResponse build();
  }
}
