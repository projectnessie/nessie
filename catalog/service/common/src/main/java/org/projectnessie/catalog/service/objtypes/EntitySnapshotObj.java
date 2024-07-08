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
package org.projectnessie.catalog.service.objtypes;

import static org.projectnessie.catalog.service.objtypes.transfer.CatalogObjIds.snapshotIdForContent;
import static org.projectnessie.versioned.storage.common.objtypes.CustomObjType.dynamicCaching;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import jakarta.annotation.Nullable;
import org.immutables.value.Value;
import org.projectnessie.catalog.model.snapshot.NessieEntitySnapshot;
import org.projectnessie.model.Content;
import org.projectnessie.model.Namespace;
import org.projectnessie.nessie.immutables.NessieImmutable;
import org.projectnessie.nessie.tasks.api.TaskObj;
import org.projectnessie.nessie.tasks.api.TaskState;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.ObjType;

/** Represents the snapshot/state of a table or view. */
@NessieImmutable
@JsonSerialize(as = ImmutableEntitySnapshotObj.class)
@JsonDeserialize(as = ImmutableEntitySnapshotObj.class)
// Suppress: "Constructor parameters should be better defined on the same level of inheritance
// hierarchy..."
@SuppressWarnings("immutables:subtype")
public interface EntitySnapshotObj extends TaskObj {

  @Override
  @Value.Default
  default ObjType type() {
    return OBJ_TYPE;
  }

  /** The Nessie {@linkplain Content content} object from which this entity snapshot was created. */
  @Nullable
  Content content();

  @Nullable
  NessieEntitySnapshot<?> snapshot();

  /** ID of the entity-object. */
  @Nullable
  ObjId entity();

  ObjType OBJ_TYPE =
      dynamicCaching(
          "catalog-snapshot",
          "c-s",
          EntitySnapshotObj.class,
          TaskObj.taskDefaultCacheExpire(),
          c -> ObjType.NOT_CACHED);

  static ObjId snapshotObjIdForContent(Content content) {
    ObjId id = snapshotIdForContent(content);
    if (id != null) {
      return id;
    }
    if (content instanceof Namespace) {
      throw new IllegalArgumentException("No snapshots for Namespace: " + content);
    }
    throw new UnsupportedOperationException(
        "Support for content with type "
            + content.getType()
            + " not implemented, content = "
            + content);
  }

  static Builder builder() {
    return ImmutableEntitySnapshotObj.builder();
  }

  interface Builder extends TaskObj.Builder {
    @CanIgnoreReturnValue
    Builder from(EntitySnapshotObj obj);

    @CanIgnoreReturnValue
    Builder id(ObjId id);

    @CanIgnoreReturnValue
    Builder snapshot(NessieEntitySnapshot<?> snapshot);

    @CanIgnoreReturnValue
    Builder entity(ObjId entity);

    @CanIgnoreReturnValue
    Builder content(Content content);

    @CanIgnoreReturnValue
    Builder taskState(TaskState taskState);

    EntitySnapshotObj build();
  }
}
