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
import org.projectnessie.versioned.persist.adapter.ContentVariant;
import org.projectnessie.versioned.persist.adapter.ContentVariantSupplier;

public class GenericContentVariantSupplier<
        CONTENT, METADATA, CONTENT_TYPE extends Enum<CONTENT_TYPE>>
    implements ContentVariantSupplier {

  private final StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker;

  public GenericContentVariantSupplier(StoreWorker<CONTENT, METADATA, CONTENT_TYPE> storeWorker) {
    this.storeWorker = storeWorker;
  }

  @Override
  public ContentVariant getContentVariant(byte type) {
    CONTENT_TYPE typeEnum = storeWorker.getType(type);
    if (storeWorker.requiresGlobalState(typeEnum)) {
      return ContentVariant.WITH_GLOBAL;
    }
    return ContentVariant.ON_REF;
  }

  @Override
  public boolean isNamespace(ByteString type) {
    return storeWorker.isNamespace(type);
  }
}
