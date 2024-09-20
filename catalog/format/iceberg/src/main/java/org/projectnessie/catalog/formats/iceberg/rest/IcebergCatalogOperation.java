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
import java.util.Optional;
import java.util.function.Function;
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
public abstract class IcebergCatalogOperation implements CatalogOperation {
  @Override
  public abstract ContentKey getKey();

  @Override
  public abstract Content.Type getType();

  @Override
  @Nullable
  public abstract String warehouse();

  public abstract List<IcebergMetadataUpdate> updates();

  public abstract List<IcebergUpdateRequirement> requirements();

  public static Builder builder() {
    return ImmutableIcebergCatalogOperation.builder();
  }

  @JsonIgnore
  @Value.Derived
  public boolean hasRequirement(Class<? extends IcebergUpdateRequirement> requirement) {
    return requirements().stream().anyMatch(requirement::isInstance);
  }

  @JsonIgnore
  @Value.Derived
  public boolean hasUpdate(Class<? extends IcebergMetadataUpdate> update) {
    return updates().stream().anyMatch(update::isInstance);
  }

  /**
   * Get the single update of the given type. Throws an exception if there are multiple updates of
   * the given type. Throws an exception if there is no update of the given type.
   */
  @JsonIgnore
  @Value.Derived
  public <T, U extends IcebergMetadataUpdate> T getSingleUpdateValue(
      Class<U> update, Function<U, T> mapper) {
    return getSingleUpdate(update, mapper)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Could not find any updates of type " + update.getSimpleName()));
  }

  @JsonIgnore
  @Value.Derived
  public <T, U extends IcebergMetadataUpdate> Optional<T> getSingleUpdate(
      Class<U> update, Function<U, T> mapper) {
    return updates().stream()
        .filter(update::isInstance)
        .map(update::cast)
        .map(mapper)
        .reduce(
            (e1, e2) -> {
              throw new IllegalArgumentException(
                  "Found multiple updates of type " + update.getSimpleName());
            });
  }

  @Value.Check
  protected void check() {
    if (hasRequirement(AssertCreate.class)) {
      if (requirements().size() > 1) {
        throw new IllegalArgumentException(
            "Invalid create requirements: "
                + requirements().stream()
                    .filter(r -> !(r instanceof AssertCreate))
                    .map(Object::getClass)
                    .map(Class::getSimpleName)
                    .collect(Collectors.joining(", ")));
      }
    }
  }

  @SuppressWarnings("unused")
  public interface Builder {
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
