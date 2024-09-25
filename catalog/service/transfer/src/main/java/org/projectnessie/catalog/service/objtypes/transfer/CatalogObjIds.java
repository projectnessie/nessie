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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.Content.Type.ICEBERG_TABLE;
import static org.projectnessie.model.Content.Type.ICEBERG_VIEW;
import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteArray;
import static org.projectnessie.versioned.storage.common.persist.ObjIdHasher.objIdHasher;

import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergContent;
import org.projectnessie.versioned.storage.common.persist.ObjId;

public final class CatalogObjIds {
  public static final ObjId LAKEHOUSE_CONFIG_ID =
      objIdFromByteArray("lakehouse-config".getBytes(UTF_8));

  private CatalogObjIds() {}

  public static ObjId snapshotIdForContent(Content content) {
    if (content.getType().equals(ICEBERG_TABLE) || content.getType().equals(ICEBERG_VIEW)) {
      IcebergContent icebergContent = (IcebergContent) content;
      return objIdHasher("ContentSnapshot")
          .hash(icebergContent.getMetadataLocation())
          .hash(icebergContent.getVersionId())
          .generate();
    }
    return null;
  }

  public static ObjId entityIdForContent(Content content) {
    if (content.getType().equals(ICEBERG_TABLE) || content.getType().equals(ICEBERG_VIEW)) {
      return objIdHasher("NessieEntity")
          .hash(requireNonNull(content.getId(), "Nessie Content has no content ID"))
          .generate();
    }
    return null;
  }
}
