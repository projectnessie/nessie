/*
 * Copyright (C) 2023 Dremio
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
 * Interface to be implemented by legacy content serializers, use {@link
 * org.projectnessie.versioned.store.ContentSerializer} for all new content types instead.
 */
@SuppressWarnings("DeprecatedIsStillUsed")
@Deprecated // for removal
public interface LegacyContentSerializer<C extends Content> extends ContentSerializer<C> {

  default boolean requiresGlobalState(ByteString onReferenceValue) {
    return false;
  }

  default Content.Type getType(ByteString onReferenceValue) {
    return contentType();
  }

  C valueFromStore(int payload, ByteString onReferenceValue, Supplier<ByteString> globalState);

  @Override
  default C valueFromStore(ByteString onReferenceValue) {
    return valueFromStore(
        payload(),
        onReferenceValue,
        () -> {
          throw new UnsupportedOperationException();
        });
  }
}
