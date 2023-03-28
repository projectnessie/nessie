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
package org.projectnessie.versioned.store;

import java.util.function.Supplier;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;

/**
 * Content serializers provide persistence layer (de)serialization functionality for a specific
 * content type.
 */
public interface ContentSerializer<C extends Content> {
  Content.Type contentType();

  byte payload();

  ByteString toStoreOnReferenceState(C content);

  C applyId(C content, String id);

  default boolean requiresGlobalState(ByteString onReferenceValue) {
    return false;
  }

  default Content.Type getType(ByteString onReferenceValue) {
    return contentType();
  }

  C valueFromStore(byte payload, ByteString onReferenceValue, Supplier<ByteString> globalState);
}
