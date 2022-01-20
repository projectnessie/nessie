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
package org.projectnessie.versioned.testworker;

import java.util.List;
import java.util.UUID;
import org.immutables.value.Value;
import org.projectnessie.versioned.ContentAttachment;

/** Content with on-reference state and mandatory global state. */
@Value.Immutable
public interface WithAttachmentsContent extends BaseContent {

  static WithAttachmentsContent withAttachments(
      List<ContentAttachment> perContent, String onRef, String contentId) {
    return ImmutableWithAttachmentsContent.builder()
        .onRef(onRef)
        .perContent(perContent)
        .id(contentId)
        .build();
  }

  static WithAttachmentsContent newWithAttachments(
      List<ContentAttachment> perContent, String onRef) {
    return ImmutableWithAttachmentsContent.builder()
        .onRef(onRef)
        .perContent(perContent)
        .id(UUID.randomUUID().toString())
        .build();
  }

  String getOnRef();

  List<ContentAttachment> getPerContent();
}
