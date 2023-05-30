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

import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
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
  public Content.Type getType(ByteString onReferenceValue) {
    ObjectTypes.Content parsed = parse(onReferenceValue);

    if (parsed.hasIcebergRefState()) {
      return Content.Type.ICEBERG_TABLE;
    }
    if (parsed.hasIcebergViewState()) {
      return Content.Type.ICEBERG_VIEW;
    }
    if (parsed.hasDeltaLakeTable()) {
      return Content.Type.DELTA_LAKE_TABLE;
    }
    if (parsed.hasNamespace()) {
      return Content.Type.NAMESPACE;
    }

    throw new IllegalArgumentException("Unsupported on-ref content " + parsed);
  }

  @Override
  public boolean requiresGlobalState(ByteString content) {
    ObjectTypes.Content parsed = parse(content);
    switch (parsed.getObjectTypeCase()) {
      case ICEBERG_REF_STATE:
        return !parsed.getIcebergRefState().hasMetadataLocation();
      case ICEBERG_VIEW_STATE:
        return !parsed.getIcebergViewState().hasMetadataLocation();
      default:
        return false;
    }
  }

  @Override
  protected Content valueFromStore(ObjectTypes.Content content, Supplier<ByteString> globalState) {
    switch (content.getObjectTypeCase()) {
      case DELTA_LAKE_TABLE:
        return valueFromStoreDeltaLakeTable(content);

      case ICEBERG_REF_STATE:
        return valueFromStoreIcebergTable(content, new IcebergMetadataPointerSupplier(globalState));

      case ICEBERG_VIEW_STATE:
        return valueFromStoreIcebergView(content, new IcebergMetadataPointerSupplier(globalState));

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
