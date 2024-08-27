/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.server.store;

import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.proto.ObjectTypes;

public final class IcebergTableSerializer extends BaseSerializer<IcebergTable> {

  @Override
  public Content.Type contentType() {
    return Content.Type.ICEBERG_TABLE;
  }

  @Override
  public int payload() {
    return 1;
  }

  @Override
  protected void toStoreOnRefState(IcebergTable table, ObjectTypes.Content.Builder builder) {
    ObjectTypes.IcebergRefState.Builder stateBuilder =
        ObjectTypes.IcebergRefState.newBuilder()
            .setSnapshotId(table.getSnapshotId())
            .setSchemaId(table.getSchemaId())
            .setSpecId(table.getSpecId())
            .setSortOrderId(table.getSortOrderId())
            .setMetadataLocation(table.getMetadataLocation());

    builder.setIcebergRefState(stateBuilder);
  }

  @Override
  protected IcebergTable valueFromStore(ObjectTypes.Content content) {
    return valueFromStoreIcebergTable(content);
  }
}
