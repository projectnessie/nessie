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
package org.projectnessie.versioned.persist.adapter;

import com.google.protobuf.ByteString;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;

/** Composite of key, content-id, content-type and content. */
@Value.Immutable
public interface KeyWithBytes {
  ContentKey getKey();

  ContentId getContentId();

  byte getPayload();

  ByteString getValue();

  static KeyWithBytes of(ContentKey key, ContentId contentId, byte payload, ByteString value) {
    return ImmutableKeyWithBytes.builder()
        .key(key)
        .contentId(contentId)
        .payload(payload)
        .value(value)
        .build();
  }
}
