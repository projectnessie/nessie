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

import static java.lang.String.format;
import static org.projectnessie.catalog.formats.iceberg.meta.IcebergTableMetadata.NO_SNAPSHOT_ID;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nullable;
import java.util.Objects;
import org.projectnessie.catalog.model.schema.NessiePartitionDefinition;
import org.projectnessie.catalog.model.schema.NessieSchema;
import org.projectnessie.catalog.model.schema.NessieSortDefinition;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.immutables.NessieImmutable;

@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertTableUUID.class,
      name = "assert-table-uuid"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertViewUUID.class,
      name = "assert-view-uuid"),
  @JsonSubTypes.Type(value = IcebergUpdateRequirement.AssertCreate.class, name = "assert-create"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertRefSnapshotId.class,
      name = "assert-ref-snapshot-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertLastAssignedFieldId.class,
      name = "assert-last-assigned-field-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertCurrentSchemaId.class,
      name = "assert-current-schema-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertLastAssignedPartitionId.class,
      name = "assert-last-assigned-partition-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertDefaultSpecId.class,
      name = "assert-default-spec-id"),
  @JsonSubTypes.Type(
      value = IcebergUpdateRequirement.AssertDefaultSortOrderId.class,
      name = "assert-default-sort-order-id"),
})
public interface IcebergUpdateRequirement {

  default void checkForTable(
      NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
    throw new UnsupportedOperationException(
        "Requirement "
            + getClass().getSimpleName().replace("Immutable", "")
            + "not supported for tables");
  }

  default void checkForView(
      NessieViewSnapshot snapshot, boolean viewExists, ContentKey contentKey) {
    throw new UnsupportedOperationException(
        "Requirement "
            + getClass().getSimpleName().replace("Immutable", "")
            + " not supported for views");
  }

  static void checkState(boolean condition, ContentKey key, String pattern, Object... args) {
    if (!condition) {
      throw new RuntimeException(new UpdateRequirementFailedException(key, format(pattern, args)));
    }
  }

  interface AssertUUID extends IcebergUpdateRequirement {
    String uuid();

    default void check(ContentKey key, NessieEntitySnapshot<?> snapshot) {
      String tableUuid = snapshot.entity().icebergUuid();
      checkState(
          Objects.equals(uuid(), tableUuid),
          key,
          "Requirement failed: UUID does not match: expected %s != %s",
          tableUuid,
          uuid());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-table-uuid")
  @JsonSerialize(as = ImmutableAssertTableUUID.class)
  @JsonDeserialize(as = ImmutableAssertTableUUID.class)
  interface AssertTableUUID extends AssertUUID {

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      check(contentKey, snapshot);
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-view-uuid")
  @JsonSerialize(as = ImmutableAssertViewUUID.class)
  @JsonDeserialize(as = ImmutableAssertViewUUID.class)
  interface AssertViewUUID extends AssertUUID {

    @Override
    default void checkForView(
        NessieViewSnapshot snapshot, boolean viewExists, ContentKey contentKey) {
      check(contentKey, snapshot);
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-create")
  @JsonSerialize(as = ImmutableAssertCreate.class)
  @JsonDeserialize(as = ImmutableAssertCreate.class)
  interface AssertCreate extends IcebergUpdateRequirement {
    static AssertCreate assertTableDoesNotExist() {
      return ImmutableAssertCreate.builder().build();
    }

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      check(tableExists, "table", contentKey);
    }

    @Override
    default void checkForView(
        NessieViewSnapshot snapshot, boolean viewExists, ContentKey contentKey) {
      check(viewExists, "table", contentKey);
    }

    default void check(boolean exists, String entityType, ContentKey contentKey) {
      checkState(
          !exists, contentKey, "Requirement failed: %s already exists: %s", entityType, contentKey);
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-ref-snapshot-id")
  @JsonSerialize(as = ImmutableAssertRefSnapshotId.class)
  @JsonDeserialize(as = ImmutableAssertRefSnapshotId.class)
  interface AssertRefSnapshotId extends IcebergUpdateRequirement {
    String ref();

    @Nullable
    Long snapshotId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      // Cannot really check the reference name, because the ref-name in a table-metadata is
      // something very different from Nessie references
      checkState(
          "main".equals(ref()),
          contentKey,
          "Requirement failed: ref must be 'main', but is '%s'",
          ref());

      Long expectedId = snapshotId();
      Long currentId = snapshot.icebergSnapshotId();
      if (expectedId != null) {
        checkState(
            Objects.equals(expectedId, currentId),
            contentKey,
            "Requirement failed: snapshot id changed: expected %s != %s",
            expectedId,
            currentId);
      } else {
        // 'null' means "no current snapshot"
        checkState(
            currentId == null || currentId == NO_SNAPSHOT_ID,
            contentKey,
            "Requirement failed: snapshot id mismatch: expected no current snapshot, but has %s",
            currentId);
      }
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-last-assigned-field-id")
  @JsonSerialize(as = ImmutableAssertLastAssignedFieldId.class)
  @JsonDeserialize(as = ImmutableAssertLastAssignedFieldId.class)
  interface AssertLastAssignedFieldId extends IcebergUpdateRequirement {
    int lastAssignedFieldId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      Integer id = snapshot.icebergLastColumnId();
      checkState(
          Objects.equals(id, lastAssignedFieldId()),
          contentKey,
          "Requirement failed: last assigned field id changed: expected %s != %s",
          id,
          lastAssignedFieldId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-current-schema-id")
  @JsonSerialize(as = ImmutableAssertCurrentSchemaId.class)
  @JsonDeserialize(as = ImmutableAssertCurrentSchemaId.class)
  interface AssertCurrentSchemaId extends IcebergUpdateRequirement {
    int currentSchemaId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      check(contentKey, snapshot);
    }

    @Override
    default void checkForView(
        NessieViewSnapshot snapshot, boolean viewExists, ContentKey contentKey) {
      check(contentKey, snapshot);
    }

    default void check(ContentKey key, NessieEntitySnapshot<?> snapshot) {
      int id = snapshot.currentSchemaObject().map(NessieSchema::icebergId).orElse(-1);
      checkState(
          currentSchemaId() == id,
          key,
          "Requirement failed: current schema changed: expected %s != %s",
          id,
          currentSchemaId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-last-assigned-partition-id")
  @JsonSerialize(as = ImmutableAssertLastAssignedPartitionId.class)
  @JsonDeserialize(as = ImmutableAssertLastAssignedPartitionId.class)
  interface AssertLastAssignedPartitionId extends IcebergUpdateRequirement {
    int lastAssignedPartitionId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      Integer id = snapshot.icebergLastPartitionId();
      checkState(
          Objects.equals(lastAssignedPartitionId(), id),
          contentKey,
          "Requirement failed: last assigned partition id changed: expected %s != %s",
          id,
          lastAssignedPartitionId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-default-spec-id")
  @JsonSerialize(as = ImmutableAssertDefaultSpecId.class)
  @JsonDeserialize(as = ImmutableAssertDefaultSpecId.class)
  interface AssertDefaultSpecId extends IcebergUpdateRequirement {
    int defaultSpecId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      int id =
          snapshot
              .currentPartitionDefinitionObject()
              .map(NessiePartitionDefinition::icebergId)
              .orElse(-1);
      checkState(
          defaultSpecId() == id,
          contentKey,
          "Requirement failed: default partition spec changed: expected %s != %s",
          id,
          defaultSpecId());
    }
  }

  @NessieImmutable
  @JsonTypeName("assert-default-sort-order-id")
  @JsonSerialize(as = ImmutableAssertDefaultSortOrderId.class)
  @JsonDeserialize(as = ImmutableAssertDefaultSortOrderId.class)
  interface AssertDefaultSortOrderId extends IcebergUpdateRequirement {
    int defaultSortOrderId();

    @Override
    default void checkForTable(
        NessieTableSnapshot snapshot, boolean tableExists, ContentKey contentKey) {
      int id =
          snapshot
              .currentSortDefinitionObject()
              .map(NessieSortDefinition::icebergSortOrderId)
              .orElse(-1);
      checkState(
          defaultSortOrderId() == id,
          contentKey,
          "Requirement failed: default sort order id changed: expected %s != %s",
          id,
          defaultSortOrderId());
    }
  }
}
