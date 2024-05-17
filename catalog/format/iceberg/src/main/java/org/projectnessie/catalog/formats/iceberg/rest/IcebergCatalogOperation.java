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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement.AssertCreate;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

/** Server-side representation of Iceberg metadata updates. */
@NessieImmutable
@JsonSerialize(as = ImmutableIcebergCatalogOperation.class)
@JsonDeserialize(as = ImmutableIcebergCatalogOperation.class)
public interface IcebergCatalogOperation extends CatalogOperation {
  @Override
  ContentKey getKey();

  @Override
  Content.Type getType();

  @Override
  @Nullable
  String warehouse();

  List<IcebergMetadataUpdate> updates();

  List<IcebergUpdateRequirement> requirements();

  static Builder builder() {
    return ImmutableIcebergCatalogOperation.builder();
  }

  @JsonIgnore
  @Value.Derived
  default boolean hasAssertCreate() {
    return requirements().stream().anyMatch(u -> u instanceof AssertCreate);
  }

  @Value.Check
  default void check() {
    if (hasAssertCreate() && requirements().size() > 1) {
      throw new IllegalArgumentException(
          "Invalid create requirements: "
              + requirements().stream()
                  .filter(r -> !(r instanceof AssertCreate))
                  .map(Object::getClass)
                  .map(Class::getSimpleName)
                  .collect(Collectors.joining(", ")));
    }
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(CatalogOperation instance);

    @CanIgnoreReturnValue
    Builder from(IcebergCatalogOperation instance);

    @CanIgnoreReturnValue
    Builder key(ContentKey key);

    @CanIgnoreReturnValue
    Builder type(Content.Type type);

    @CanIgnoreReturnValue
    Builder warehouse(String warehouse);

    @CanIgnoreReturnValue
    Builder addUpdate(IcebergMetadataUpdate element);

    @CanIgnoreReturnValue
    Builder addUpdates(IcebergMetadataUpdate... elements);

    @CanIgnoreReturnValue
    Builder updates(Iterable<? extends IcebergMetadataUpdate> elements);

    @CanIgnoreReturnValue
    Builder addAllUpdates(Iterable<? extends IcebergMetadataUpdate> elements);

    @CanIgnoreReturnValue
    Builder addRequirement(IcebergUpdateRequirement element);

    @CanIgnoreReturnValue
    Builder addRequirements(IcebergUpdateRequirement... elements);

    @CanIgnoreReturnValue
    Builder requirements(Iterable<? extends IcebergUpdateRequirement> elements);

    @CanIgnoreReturnValue
    Builder addAllRequirements(Iterable<? extends IcebergUpdateRequirement> elements);

    IcebergCatalogOperation build();
  }
}
