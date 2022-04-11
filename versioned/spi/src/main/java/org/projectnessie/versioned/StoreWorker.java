/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned;

import com.google.protobuf.ByteString;
import java.util.function.Supplier;

/**
 * A set of helpers that users of a VersionStore must implement.
 *
 * @param <CONTENT> The value type saved in the VersionStore.
 * @param <COMMIT_METADATA> The commit metadata type saved in the VersionStore.
 */
public interface StoreWorker<CONTENT, COMMIT_METADATA, CONTENT_TYPE extends Enum<CONTENT_TYPE>> {

  /** Returns the serialized representation of the on-reference part of the given content-object. */
  ByteString toStoreOnReferenceState(CONTENT content);

  ByteString toStoreGlobalState(CONTENT content);

  CONTENT valueFromStore(ByteString onReferenceValue, Supplier<ByteString> globalState);

  String getId(CONTENT content);

  Byte getPayload(CONTENT content);

  boolean requiresGlobalState(ByteString content);

  boolean requiresGlobalState(CONTENT content);

  CONTENT_TYPE getType(ByteString onRefContent);

  CONTENT_TYPE getType(Byte payload);

  default CONTENT_TYPE getType(CONTENT content) {
    return getType(getPayload(content));
  }

  Serializer<COMMIT_METADATA> getMetadataSerializer();

  default boolean isNamespace(ByteString type) {
    return false;
  }
}
