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
package org.projectnessie.catalog.model.snapshot;

import static java.util.function.Function.identity;
import static org.projectnessie.catalog.model.schema.NessieSchema.NO_SCHEMA_ID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.NessieEntity;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.model.CommitMeta;

/**
 * Common entity, table or view, attributes for a given snapshot of that entity. An entity snapshot
 * is the state of an entity on a particular Nessie commit.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = NessieTableSnapshot.class, name = "TABLE"),
  @JsonSubTypes.Type(value = NessieViewSnapshot.class, name = "VIEW")
})
public interface NessieEntitySnapshot<E extends NessieEntity> {

  NessieEntitySnapshot<E> withId(NessieId id);

  @JsonTypeId
  String type();

  NessieId id();

  Map<String, String> properties();

  E entity();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  NessieId currentSchemaId();

  List<NessieSchema> schemas();

  @Value.Lazy
  default Map<Integer, NessieSchema> schemaByIcebergId() {
    return schemas().stream()
        .filter(s -> s.icebergId() != NO_SCHEMA_ID)
        .collect(Collectors.toMap(NessieSchema::icebergId, identity()));
  }

  default Optional<NessieSchema> schemaByIcebergId(int schemaId) {
    return schemas().stream().filter(s -> s.icebergId() == schemaId).findFirst();
  }

  @Value.Lazy
  @JsonIgnore
  default Optional<NessieSchema> currentSchemaObject() {
    for (NessieSchema schema : schemas()) {
      if (schema.id().equals(currentSchemaId())) {
        return Optional.of(schema);
      }
    }
    return Optional.empty();
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  Integer icebergFormatVersion();

  /**
   * The Iceberg entity base location, usually something like {@code
   * s3://bucket1/warehouse/ns/table_<uuid>}.
   */
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergLocation();

  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  @jakarta.annotation.Nullable
  // Can be null, if for example no Iceberg snapshot exists in a table-metadata
  @JsonSerialize(using = CommitMeta.InstantSerializer.class)
  @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
  Instant snapshotCreatedTimestamp();

  @JsonSerialize(using = CommitMeta.InstantSerializer.class)
  @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
  // FIXME is this nullable? The builder method says yes, but the interface says no.
  Instant lastUpdatedTimestamp();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  Set<String> additionalKnownLocations();

  @SuppressWarnings("unused")
  interface Builder<B extends Builder<B>> {
    @CanIgnoreReturnValue
    B id(NessieId id);

    @CanIgnoreReturnValue
    B putProperty(String key, String value);

    @CanIgnoreReturnValue
    B putProperty(Map.Entry<String, ? extends String> entry);

    @CanIgnoreReturnValue
    B properties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B putAllProperties(Map<String, ? extends String> entries);

    @CanIgnoreReturnValue
    B currentSchemaId(@Nullable NessieId currentSchemaId);

    @CanIgnoreReturnValue
    B addSchema(NessieSchema element);

    @CanIgnoreReturnValue
    B addSchemas(NessieSchema... elements);

    @CanIgnoreReturnValue
    B schemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    B addAllSchemas(Iterable<? extends NessieSchema> elements);

    @CanIgnoreReturnValue
    B icebergFormatVersion(@Nullable Integer icebergFormatVersion);

    @CanIgnoreReturnValue
    B snapshotCreatedTimestamp(@Nullable Instant snapshotCreatedTimestamp);

    @CanIgnoreReturnValue
    B lastUpdatedTimestamp(@Nullable Instant lastUpdatedTimestamp);

    @CanIgnoreReturnValue
    B icebergLocation(String icebergLocation);

    @CanIgnoreReturnValue
    B addAdditionalKnownLocation(String element);

    @CanIgnoreReturnValue
    B addAdditionalKnownLocations(String... elements);

    @CanIgnoreReturnValue
    B additionalKnownLocations(Iterable<String> elements);

    @CanIgnoreReturnValue
    B addAllAdditionalKnownLocations(Iterable<String> elements);
  }
}
