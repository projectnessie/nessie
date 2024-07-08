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

import static java.util.Objects.requireNonNull;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class CatalogObjIds {
  private CatalogObjIds() {}

  public static ObjId snapshotIdForContent(Content content) {
    if (content instanceof IcebergTable) {
      IcebergTable icebergTable = (IcebergTable) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergTable.getMetadataLocation())
          .hash(icebergTable.getSnapshotId())
          .generate();
    }
    if (content instanceof IcebergView) {
      IcebergView icebergView = (IcebergView) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergView.getMetadataLocation())
          .hash(icebergView.getVersionId())
          .generate();
    }
    return null;
  }

  public static ObjId entityIdForContent(Content content) {
    if (content instanceof IcebergTable || content instanceof IcebergView) {
      return objIdHasher("NessieEntity")
          .hash(requireNonNull(content.getId(), "Nessie Content has no content ID"))
          .generate();
    }
    return null;
  }
}
