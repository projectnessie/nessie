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
package org.projectnessie.catalog.formats.iceberg.nessie;

import static java.time.Instant.now;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.model.snapshot.NessieViewSnapshot;
import org.projectnessie.model.ContentKey;

/**
 * Maintains state when applying {@linkplain IcebergMetadataUpdate Iceberg metadata updates} to a
 * {@linkplain org.projectnessie.catalog.model.snapshot.NessieViewSnapshot view snapshot}.
 *
 * <p>State includes:
 *
 * <ul>
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSchema added schema} id for {@link
 *       IcebergMetadataUpdate.SetCurrentSchema SetCurrentSchema}
 * </ul>
 */
public class IcebergViewMetadataUpdateState {
  private final NessieViewSnapshot.Builder builder;
  private final ContentKey key;
  private final boolean viewExists;

  private NessieViewSnapshot snapshot;
  private int lastAddedSchemaId = -1;
  private long lastAddedVersionId = -1;
  private final List<IcebergSnapshot> addedSnapshots = new ArrayList<>();
  private final Set<Integer> addedSchemaIds = new HashSet<>();
  private final Set<Long> addedVersionIds = new HashSet<>();
  private final Set<CatalogOps> catalogOps = EnumSet.noneOf(CatalogOps.class);

  public IcebergViewMetadataUpdateState(
      NessieViewSnapshot snapshot, ContentKey key, boolean viewExists) {
    this.snapshot = snapshot;
    this.builder = NessieViewSnapshot.builder().from(snapshot);
    this.key = key;
    this.viewExists = viewExists;
  }

  public NessieViewSnapshot.Builder builder() {
    return builder;
  }

  public void addCatalogOp(CatalogOps op) {
    catalogOps.add(op);
  }

  public Set<CatalogOps> catalogOps() {
    return catalogOps;
  }

  public NessieViewSnapshot snapshot() {
    return snapshot;
  }

  public int lastAddedSchemaId() {
    return lastAddedSchemaId;
  }

  public void schemaAdded(int schemaId) {
    if (schemaId >= 0) {
      addedSchemaIds.add(schemaId);
    }
    lastAddedSchemaId = schemaId;
  }

  public boolean isAddedSchema(int schemaId) {
    return addedSchemaIds.contains(schemaId);
  }

  public long lastAddedVersionId() {
    return lastAddedVersionId;
  }

  public void versionAdded(long versionId) {
    if (versionId >= 0) {
      addedVersionIds.add(versionId);
    }
    lastAddedVersionId = versionId;
  }

  public boolean isAddedVersion(long versionId) {
    return addedVersionIds.contains(versionId);
  }

  public IcebergViewMetadataUpdateState checkRequirements(
      List<IcebergUpdateRequirement> requirements) {
    for (IcebergUpdateRequirement requirement : requirements) {
      requirement.checkForView(snapshot, viewExists, key);
    }
    return this;
  }

  public IcebergViewMetadataUpdateState applyUpdates(List<IcebergMetadataUpdate> updates) {
    Instant now = now();
    for (IcebergMetadataUpdate update : updates) {
      update.applyToView(this);
      snapshot = builder.lastUpdatedTimestamp(now).build();
    }
    return this;
  }
}
