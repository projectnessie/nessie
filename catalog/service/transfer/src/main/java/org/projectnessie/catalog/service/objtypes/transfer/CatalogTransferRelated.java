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
package org.projectnessie.catalog.service.objtypes.transfer;

import static java.util.Collections.emptySet;

import java.util.Set;
import org.projectnessie.model.Content;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.transfer.related.TransferRelatedObjects;

public class CatalogTransferRelated implements TransferRelatedObjects {
  @Override
  public Set<ObjId> contentRelatedObjects(Content content) {
    ObjId entityId = CatalogObjIds.entityIdForContent(content);
    ObjId snapshotId = CatalogObjIds.snapshotIdForContent(content);
    if (snapshotId != null && entityId != null) {
      return Set.of(entityId, snapshotId);
    }
    if (snapshotId != null) {
      return Set.of(snapshotId);
    }
    if (entityId != null) {
      return Set.of(entityId);
    }
    return emptySet();
  }

  @Override
  public Set<ObjId> repositoryRelatedObjects() {
    return Set.of(CatalogObjIds.LAKEHOUSE_CONFIG_ID);
  }
}
