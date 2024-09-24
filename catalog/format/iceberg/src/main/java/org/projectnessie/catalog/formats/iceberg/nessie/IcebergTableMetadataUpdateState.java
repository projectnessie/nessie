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
import static java.util.Collections.emptyMap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.projectnessie.catalog.formats.iceberg.meta.IcebergSnapshot;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergMetadataUpdate;
import org.projectnessie.catalog.formats.iceberg.rest.IcebergUpdateRequirement;
import org.projectnessie.catalog.model.id.NessieId;
import org.projectnessie.catalog.model.snapshot.NessieTableSnapshot;
import org.projectnessie.model.ContentKey;

/**
 * Maintains state when applying {@linkplain IcebergMetadataUpdate Iceberg metadata updates} to a
 * {@linkplain NessieTableSnapshot table snapshot}.
 *
 * <p>State includes:
 *
 * <ul>
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSchema added schema} id for {@link
 *       IcebergMetadataUpdate.SetCurrentSchema SetCurrentSchema}
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddPartitionSpec added partition spec} id for {@link
 *       IcebergMetadataUpdate.SetDefaultPartitionSpec SetDefaultPartitionSpec}
 *   <li>Last {@linkplain IcebergMetadataUpdate.AddSortOrder added sort order} id for {@link
 *       IcebergMetadataUpdate.SetDefaultSortOrder SetDefaultSortOrder}
 * </ul>
 */
public class IcebergTableMetadataUpdateState {
  private final NessieTableSnapshot.Builder builder;
  private final ContentKey key;
  private final boolean tableExists;

  private NessieTableSnapshot snapshot;
  private int lastAddedSchemaId = -1;
  private int lastAddedSpecId = -1;
  private int lastAddedOrderId = -1;
  private final List<IcebergSnapshot> addedSnapshots = new ArrayList<>();
  private final Set<Integer> addedSchemaIds = new HashSet<>();
  private final Set<Integer> addedSpecIds = new HashSet<>();
  private final Set<Integer> addedOrderIds = new HashSet<>();
  private final Set<CatalogOps> catalogOps = EnumSet.noneOf(CatalogOps.class);

  public IcebergTableMetadataUpdateState(
      NessieTableSnapshot snapshot, ContentKey key, boolean tableExists) {
    this.snapshot = snapshot;
    this.builder = NessieTableSnapshot.builder().from(snapshot);
    this.key = key;
    this.tableExists = tableExists;
  }

  public NessieTableSnapshot.Builder builder() {
    return builder;
  }

  public void addCatalogOp(CatalogOps op) {
    catalogOps.add(op);
  }

  public Set<CatalogOps> catalogOps() {
    return catalogOps;
  }

  public NessieTableSnapshot snapshot() {
    return snapshot;
  }

  public List<IcebergSnapshot> addedSnapshots() {
    return addedSnapshots;
  }

  public void snapshotAdded(IcebergSnapshot snapshot) {
    addedSnapshots.add(snapshot);
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

  public int lastAddedSpecId() {
    return lastAddedSpecId;
  }

  public void specAdded(int specId) {
    if (specId >= 0) {
      addedSpecIds.add(specId);
    }
    lastAddedSpecId = specId;
  }

  public boolean isAddedSpec(int specId) {
    return addedSpecIds.contains(specId);
  }

  public int lastAddedOrderId() {
    return lastAddedOrderId;
  }

  public void sortOrderAdded(int orderId) {
    if (orderId >= 0) {
      addedOrderIds.add(orderId);
    }
    lastAddedOrderId = orderId;
  }

  public boolean isAddedOrder(int orderId) {
    return addedOrderIds.contains(orderId);
  }

  public IcebergTableMetadataUpdateState checkRequirements(
      List<IcebergUpdateRequirement> requirements) {
    for (IcebergUpdateRequirement requirement : requirements) {
      requirement.checkForTable(snapshot, tableExists, key);
    }
    return this;
  }

  public IcebergTableMetadataUpdateState applyUpdates(List<IcebergMetadataUpdate> updates) {
    Instant now = now();
    for (IcebergMetadataUpdate update : updates) {
      update.applyToTable(this);
      snapshot = builder.lastUpdatedTimestamp(now).build();
    }
    return this;
  }

  private Map<Integer, Integer> remappedFieldIds;

  public void remappedFields(Map<Integer, Integer> remappedFieldIds) {
    this.remappedFieldIds = remappedFieldIds;
  }

  public int mapFieldId(int sourceId, NessieId schemaId) {
    Map<Integer, Integer> map = remappedFieldIds;
    if (map == null) {
      map = emptyMap();
    }
    return map.getOrDefault(sourceId, sourceId);
  }
}
