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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergTableIdentifier;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergUpdateTableRequest.class)
@JsonDeserialize(as = ImmutableIcebergUpdateTableRequest.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergUpdateTableRequest extends IcebergUpdateEntityRequest {

  static Builder builder() {
    return ImmutableIcebergUpdateTableRequest.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(IcebergUpdateTableRequest instance);

    @CanIgnoreReturnValue
    Builder addRequirement(IcebergUpdateRequirement element);

    @CanIgnoreReturnValue
    Builder addRequirements(IcebergUpdateRequirement... elements);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder requirements(Iterable<? extends IcebergUpdateRequirement> elements);

    @CanIgnoreReturnValue
    Builder addAllRequirements(Iterable<? extends IcebergUpdateRequirement> elements);

    @CanIgnoreReturnValue
    Builder addUpdate(IcebergMetadataUpdate element);

    @CanIgnoreReturnValue
    Builder addUpdates(IcebergMetadataUpdate... elements);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder updates(Iterable<? extends IcebergMetadataUpdate> elements);

    @CanIgnoreReturnValue
    Builder addAllUpdates(Iterable<? extends IcebergMetadataUpdate> elements);

    @CanIgnoreReturnValue
    @JsonProperty
    Builder identifier(IcebergTableIdentifier identifier);

    IcebergUpdateTableRequest build();
  }
}
