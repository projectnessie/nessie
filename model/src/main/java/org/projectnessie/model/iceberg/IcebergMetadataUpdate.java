/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.model.iceberg;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.immutables.value.Value;

public interface IcebergMetadataUpdate {
  @Value.Immutable
  @JsonSerialize(as = ImmutableAssignUUID.class)
  @JsonDeserialize(as = ImmutableAssignUUID.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata assign UUID")
  interface AssignUUID extends IcebergMetadataUpdate {
    String getUuid();

    static ImmutableAssignUUID.Builder builder() {
      return ImmutableAssignUUID.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableUpgradeFormatVersion.class)
  @JsonDeserialize(as = ImmutableUpgradeFormatVersion.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata upgrade format version")
  interface UpgradeFormatVersion extends IcebergMetadataUpdate {
    int getFormatVersion();

    static ImmutableUpgradeFormatVersion.Builder builder() {
      return ImmutableUpgradeFormatVersion.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableAddSchema.class)
  @JsonDeserialize(as = ImmutableAddSchema.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata add schema")
  interface AddSchema extends IcebergMetadataUpdate {
    int getSchemaId();

    int getLastColumnId();

    static ImmutableAddSchema.Builder builder() {
      return ImmutableAddSchema.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetCurrentSchema.class)
  @JsonDeserialize(as = ImmutableSetCurrentSchema.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set current schema")
  interface SetCurrentSchema extends IcebergMetadataUpdate {
    int getSchemaId();

    static ImmutableSetCurrentSchema.Builder builder() {
      return ImmutableSetCurrentSchema.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableAddPartitionSpec.class)
  @JsonDeserialize(as = ImmutableAddPartitionSpec.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata add partition spec")
  interface AddPartitionSpec extends IcebergMetadataUpdate {
    int getSpecId();

    static ImmutableAddPartitionSpec.Builder builder() {
      return ImmutableAddPartitionSpec.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetDefaultPartitionSpec.class)
  @JsonDeserialize(as = ImmutableSetDefaultPartitionSpec.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set default partition spec")
  interface SetDefaultPartitionSpec extends IcebergMetadataUpdate {
    int getSpecId();

    static ImmutableSetDefaultPartitionSpec.Builder builder() {
      return ImmutableSetDefaultPartitionSpec.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableAddSortOrder.class)
  @JsonDeserialize(as = ImmutableAddSortOrder.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata add sort order")
  interface AddSortOrder extends IcebergMetadataUpdate {
    int getSortOrderId();

    static ImmutableAddSortOrder.Builder builder() {
      return ImmutableAddSortOrder.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetDefaultSortOrder.class)
  @JsonDeserialize(as = ImmutableSetDefaultSortOrder.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set default sort order")
  interface SetDefaultSortOrder extends IcebergMetadataUpdate {
    int getSortOrderId();

    static ImmutableSetDefaultSortOrder.Builder builder() {
      return ImmutableSetDefaultSortOrder.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableAddSnapshot.class)
  @JsonDeserialize(as = ImmutableAddSnapshot.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata add snapshot")
  interface AddSnapshot extends IcebergMetadataUpdate {
    long getSnapshotId();

    static ImmutableAddSnapshot.Builder builder() {
      return ImmutableAddSnapshot.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableRemoveSnapshot.class)
  @JsonDeserialize(as = ImmutableRemoveSnapshot.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata remove snapshot")
  interface RemoveSnapshot extends IcebergMetadataUpdate {
    long getSnapshotId();

    static ImmutableRemoveSnapshot.Builder builder() {
      return ImmutableRemoveSnapshot.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetCurrentSnapshot.class)
  @JsonDeserialize(as = ImmutableSetCurrentSnapshot.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set current snapshot")
  interface SetCurrentSnapshot extends IcebergMetadataUpdate {
    Long getSnapshotId();

    static ImmutableSetCurrentSnapshot.Builder builder() {
      return ImmutableSetCurrentSnapshot.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetProperties.class)
  @JsonDeserialize(as = ImmutableSetProperties.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set properties")
  interface SetProperties extends IcebergMetadataUpdate {
    Map<String, String> getUpdated();

    static ImmutableSetProperties.Builder builder() {
      return ImmutableSetProperties.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableRemoveProperties.class)
  @JsonDeserialize(as = ImmutableRemoveProperties.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata remove properties")
  interface RemoveProperties extends IcebergMetadataUpdate {
    Set<String> getRemoved();

    static ImmutableRemoveProperties.Builder builder() {
      return ImmutableRemoveProperties.builder();
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSetLocation.class)
  @JsonDeserialize(as = ImmutableSetLocation.class)
  @org.eclipse.microprofile.openapi.annotations.media.Schema(
      type = SchemaType.OBJECT,
      title = "Iceberg metadata set location")
  interface SetLocation extends IcebergMetadataUpdate {
    @Nullable
    String getLocation();

    static ImmutableSetLocation.Builder builder() {
      return ImmutableSetLocation.builder();
    }
  }
}
