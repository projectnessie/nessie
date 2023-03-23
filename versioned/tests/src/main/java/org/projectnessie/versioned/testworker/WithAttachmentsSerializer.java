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

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.versioned.testworker.WithAttachmentsContent.withAttachments;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachmentKey;

public class WithAttachmentsSerializer extends TestContentSerializer<WithAttachmentsContent> {

  @Override
  public Content.Type contentType() {
    return WithAttachmentsContent.WITH_ATTACHMENTS;
  }

  @Override
  public byte payload() {
    return 126;
  }

  @Override
  public ByteString toStoreOnReferenceState(
      WithAttachmentsContent content, Consumer<ContentAttachment> attachmentConsumer) {
    String value = content.getOnRef();
    content.getPerContent().forEach(attachmentConsumer);
    return ByteString.copyFromUtf8(content.getType().name() + ":" + content.getId() + ":" + value);
  }

  @Override
  public WithAttachmentsContent applyId(WithAttachmentsContent content, String id) {
    return WithAttachmentsContent.withAttachments(content.getPerContent(), content.getOnRef(), id);
  }

  @Override
  protected WithAttachmentsContent valueFromStore(
      String contentId,
      String onRef,
      ByteString global,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever) {
    assertThat(global).isNull();
    Stream<ContentAttachmentKey> keys = Stream.empty();
    try (Stream<ContentAttachment> attachments = attachmentsRetriever.apply(keys)) {
      assertThat(attachments).isNotEmpty();
      return withAttachments(attachments.collect(Collectors.toList()), onRef, contentId);
    }
  }
}
