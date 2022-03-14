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
package org.projectnessie.versioned.persist.store;

import com.google.protobuf.ByteString;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.ContentType;
import org.projectnessie.versioned.persist.adapter.ContentTypeSupplier;

public class GenericContentTypeSupplier<CONTENT, METADATA, CONTENT_TYPE extends Enum<CONTENT_TYPE>>
    implements ContentTypeSupplier {

  private final StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker;

  public GenericContentTypeSupplier(StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker) {
    this.storeWorker = storeWorker;
  }

  @Override
  public ContentType getContentType(ByteString type) {
    CONTENT_TYPE typeEnum = storeWorker.getType(type);
    if (typeEnum.name().equals("ICEBERG_TABLE")) {
      return ContentType.ICEBERG_TABLE;
    }
    if (typeEnum.name().equals("ICEBERG_VIEW")) {
      return ContentType.ICEBERG_VIEW;
    }
    if (typeEnum.name().equals("DELTA_LAKE_TABLE")) {
      return ContentType.DELTA_LAKE_TABLE;
    }
    return ContentType.UNKNOWN;
  }
}
