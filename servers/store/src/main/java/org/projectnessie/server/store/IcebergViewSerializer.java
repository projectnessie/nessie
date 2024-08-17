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
import org.projectnessie.model.IcebergView;
import org.projectnessie.server.store.proto.ObjectTypes;

public final class IcebergViewSerializer extends BaseSerializer<IcebergView> {

  @Override
  public Content.Type contentType() {
    return Content.Type.ICEBERG_VIEW;
  }

  @Override
  public int payload() {
    return 3;
  }

  @Override
  @SuppressWarnings("deprecation")
  protected void toStoreOnRefState(IcebergView view, ObjectTypes.Content.Builder builder) {
    ObjectTypes.IcebergViewState.Builder stateBuilder =
        ObjectTypes.IcebergViewState.newBuilder()
            .setVersionId(view.getVersionId())
            .setSchemaId(view.getSchemaId());
    String dialect = view.getDialect();
    String sqlText = view.getSqlText();
    String metadataLocation = view.getMetadataLocation();
    if (dialect != null) {
      stateBuilder.setDialect(dialect);
    }
    if (sqlText != null) {
      stateBuilder.setSqlText(sqlText);
    }
    if (metadataLocation != null) {
      stateBuilder.setMetadataLocation(metadataLocation);
    }

    builder.setIcebergViewState(stateBuilder);
  }

  @Override
  protected IcebergView valueFromStore(ObjectTypes.Content content) {
    return valueFromStoreIcebergView(content);
  }
}
