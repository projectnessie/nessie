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

import com.google.common.base.Preconditions;
import javax.validation.constraints.NotBlank;
import org.immutables.value.Value;

/** Represents the content attachment key in Nessie. */
@Value.Immutable
public interface ContentAttachmentKey {

  /** Content ID to which the binary object identified by this key belongs to. */
  @NotBlank
  String getContentId();

  /** The object type, for example {@code snapshot} or {@code schema}. */
  @NotBlank
  String getAttachmentType();

  /** The object ID, for example the snapshot-ID or schema-ID. */
  @NotBlank
  String getAttachmentId();

  @Value.Check
  default void check() {
    // '::' is used to separate the elements of this composite on the database-adapter level
    Preconditions.checkState(
        getContentId().indexOf(':') == -1
            && getAttachmentType().indexOf(':') == -1
            && getAttachmentId().indexOf(':') == -1,
        "Elements of ContentAttachmentKey must not contain ':' characters, content-id='%s', object-type=':', object-ID='%s'",
        getContentId(),
        getAttachmentType(),
        getAttachmentId());
  }

  static ImmutableContentAttachmentKey.Builder builder() {
    return ImmutableContentAttachmentKey.builder();
  }

  static ContentAttachmentKey of(String contentId, String attachmentType, String attachmentId) {
    return builder()
        .contentId(contentId)
        .attachmentType(attachmentType)
        .attachmentId(attachmentId)
        .build();
  }

  default String asString() {
    return keyPartsAsString(getContentId(), getAttachmentType(), getAttachmentId());
  }

  static String keyPartsAsString(String contentId, String objectType, String objectId) {
    Preconditions.checkState(
        !contentId.contains("::") && !objectType.contains("::") && !objectId.contains("::"),
        "Attributes of an attachment key must not contain '::'");
    return contentId + "::" + objectType + "::" + objectId;
  }
}
