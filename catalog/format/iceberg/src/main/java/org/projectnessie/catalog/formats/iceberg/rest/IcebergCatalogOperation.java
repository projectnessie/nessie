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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.immutables.value.Value;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement.AssertCreate;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.catalog.model.ops.CatalogOperationType;
import org.projectnessie.model.Content;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

/**
 * Server-side, internal representation of Iceberg metadata updates on tables and views. Not meant
 * to be serialized/deserialized.
 */
@NessieImmutable
public abstract class IcebergCatalogOperation implements CatalogOperation<IcebergMetadataUpdate> {

  @Value.Default
  @Override
  public CatalogOperationType getOperationType() {
    // note: the default impl cannot detect DROP operations
    boolean hasAssertCreate = hasRequirement(AssertCreate.class);
    if (getContentType().equals(Type.ICEBERG_TABLE)) {
      return hasAssertCreate ? CatalogOperationType.CREATE_TABLE : CatalogOperationType.ALTER_TABLE;
    } else if (getContentType().equals(Type.ICEBERG_VIEW)) {
      return hasAssertCreate ? CatalogOperationType.CREATE_VIEW : CatalogOperationType.ALTER_VIEW;
    } else {
      throw new IllegalArgumentException("Unsupported content type: " + getContentType());
    }
  }

  @Override
  public abstract ContentKey getContentKey();

  @Override
  public abstract Content.Type getContentType();

  /**
   * The logical warehouse name where this operation will occur. Must correspond to a warehouse
   * configured under {@code nessie.catalog.warehouses.<name>}.
   *
   * <p>If not set, the default warehouse will be used.
   */
  @Nullable
  public abstract String warehouse();

  @Override
  public abstract List<IcebergMetadataUpdate> getUpdates();

  public abstract List<IcebergUpdateRequirement> getRequirements();

  public static Builder builder() {
    return ImmutableIcebergCatalogOperation.builder();
  }

  @JsonIgnore
  @Value.Derived
  public boolean hasRequirement(Class<? extends IcebergUpdateRequirement> requirement) {
    return getRequirements().stream().anyMatch(requirement::isInstance);
  }

  @JsonIgnore
  @Value.Derived
  public boolean hasUpdate(Class<? extends IcebergMetadataUpdate> update) {
    return getUpdates().stream().anyMatch(update::isInstance);
  }

  /**
   * Get the single update of the given type. Throws an exception if there are multiple updates of
   * the given type. Throws an exception if there is no update of the given type.
   */
  @JsonIgnore
  @Value.Derived
  public <T, U extends IcebergMetadataUpdate> T getSingleUpdateValue(
      Class<U> update, Function<U, T> mapper) {
    return getUpdates().stream()
        .filter(update::isInstance)
        .map(update::cast)
        .map(mapper)
        .reduce(
            (e1, e2) -> {
              throw new IllegalArgumentException(
                  "Found multiple updates of type " + update.getSimpleName());
            })
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Could not find any updates of type " + update.getSimpleName()));
  }

  @Value.Check
  protected void check() {
    if (hasRequirement(AssertCreate.class)) {
      if (getRequirements().size() > 1) {
        throw new IllegalArgumentException(
            "Invalid create requirements: "
                + getRequirements().stream()
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
    Builder from(IcebergCatalogOperation instance);

    @CanIgnoreReturnValue
    Builder operationType(CatalogOperationType type);

    @CanIgnoreReturnValue
    Builder contentKey(ContentKey key);

    @CanIgnoreReturnValue
    Builder contentType(Content.Type type);

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
