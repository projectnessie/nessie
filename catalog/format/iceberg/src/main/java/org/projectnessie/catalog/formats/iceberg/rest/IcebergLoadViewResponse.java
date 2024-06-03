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
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.Map;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergViewMetadata;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergLoadViewResponse.class)
@JsonDeserialize(as = ImmutableIcebergLoadViewResponse.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergLoadViewResponse {

  String metadataLocation();

  IcebergViewMetadata metadata();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Map<String, String> config();

  static Builder builder() {
    return ImmutableIcebergLoadViewResponse.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergLoadViewResponse instance);

    @CanIgnoreReturnValue
    Builder metadataLocation(String metadataLocation);

    @CanIgnoreReturnValue
    Builder metadata(IcebergViewMetadata metadata);

    @CanIgnoreReturnValue
    Builder putConfig(String key, String value);

    @CanIgnoreReturnValue
    Builder putConfig(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    Builder config(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    Builder putAllConfig(Map<String, ? extends String> entries);

    IcebergLoadViewResponse build();
  }
}
