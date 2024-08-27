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
import org.projectnessie.server.store.proto.ObjectTypes;

/**
 * Provides content serialization functionality for the case when old Nessie versions persisted
 * contents with payload {@code 0}.
 */
public final class UnknownSerializer extends BaseSerializer<Content> {

  @Override
  public Content.Type contentType() {
    return Content.Type.UNKNOWN;
  }

  @Override
  public int payload() {
    return 0;
  }

  @Override
  protected void toStoreOnRefState(Content table, ObjectTypes.Content.Builder builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Content valueFromStore(ObjectTypes.Content content) {
    switch (content.getObjectTypeCase()) {
      case DELTA_LAKE_TABLE:
        return valueFromStoreDeltaLakeTable(content);

      case ICEBERG_REF_STATE:
        return valueFromStoreIcebergTable(content);

      case ICEBERG_VIEW_STATE:
        return valueFromStoreIcebergView(content);

      case NAMESPACE:
        return valueFromStoreNamespace(content);

      case UDF:
        return valueFromStoreUDF(content);

      case OBJECTTYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Unknown type " + content.getObjectTypeCase());
    }
  }
}
