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
package org.projectnessie.versioned;

import org.immutables.value.Value;

/** Represents the content attachment key in Nessie. */
@Value.Immutable
public interface ContentAttachmentKey {

  /** Content ID to which the binary object identified by this key belongs to. */
  String getContentId();

  /** The object type, for example {@code snapshot} or {@code schema}. */
  String getObjectType();

  /** The object ID, for example the snapshot-ID or schema-ID. */
  String getObjectId();

  static ImmutableContentAttachmentKey.Builder builder() {
    return ImmutableContentAttachmentKey.builder();
  }

  static ContentAttachmentKey of(String contentId, String objectType, String objectId) {
    return builder().contentId(contentId).objectType(objectType).objectId(objectId).build();
  }
}
