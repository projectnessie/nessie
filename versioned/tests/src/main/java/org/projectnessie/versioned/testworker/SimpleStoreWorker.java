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
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;
import static org.projectnessie.versioned.testworker.WithAttachmentsContent.withAttachments;
import static org.projectnessie.versioned.testworker.WithGlobalStateContent.withGlobal;

import com.google.protobuf.ByteString;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;
import org.projectnessie.versioned.ContentAttachment;
import org.projectnessie.versioned.ContentAttachmentKey;
import org.projectnessie.versioned.StoreWorker;

/**
 * {@link StoreWorker} implementation for tests using types that are independent of those in the
 * {@code nessie-model} Maven module.
 */
public final class SimpleStoreWorker implements StoreWorker {

  public static final SimpleStoreWorker INSTANCE = new SimpleStoreWorker();

  @Override
  public ByteString toStoreOnReferenceState(
      Content content, Consumer<ContentAttachment> attachmentConsumer) {
    Content.Type type = content.getType();
    String value;
    if (type.equals(OnRefOnly.ON_REF_ONLY)) {
      value = ((OnRefOnly) content).getOnRef();
    } else if (type.equals(WithGlobalStateContent.WITH_GLOBAL_STATE)) {
      value = ((WithGlobalStateContent) content).getOnRef();
    } else if (type.equals(WithAttachmentsContent.WITH_ATTACHMENTS)) {
      value = ((WithAttachmentsContent) content).getOnRef();
    } else {
      throw new IllegalArgumentException("" + content);
    }

    if (content instanceof WithAttachmentsContent) {
      ((WithAttachmentsContent) content).getPerContent().forEach(attachmentConsumer);
    }

    return ByteString.copyFromUtf8(content.getType().name() + ":" + content.getId() + ":" + value);
  }

  @Override
  public ByteString toStoreGlobalState(Content content) {
    if (content instanceof WithGlobalStateContent) {
      return ByteString.copyFromUtf8(((WithGlobalStateContent) content).getGlobal());
    }
    throw new IllegalArgumentException();
  }

  @Override
  public Content valueFromStore(
      ByteString onReferenceValue,
      Supplier<ByteString> globalState,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever) {
    String serialized = onReferenceValue.toStringUtf8();

    int i = serialized.indexOf(':');
    String typeString = serialized.substring(0, i);
    serialized = serialized.substring(i + 1);
    Content.Type type = ContentTypes.forName(typeString);

    i = serialized.indexOf(':');
    String contentId = serialized.substring(0, i);
    i = serialized.indexOf(':');
    String onRef = serialized.substring(i + 1);

    ByteString global = globalState.get();
    if (type.equals(OnRefOnly.ON_REF_ONLY)) {
      assertThat(global).isNull();
      return onRef(onRef, contentId);
    }
    if (type.equals(WithGlobalStateContent.WITH_GLOBAL_STATE)) {
      assertThat(global).isNotNull();
      return withGlobal(global.toStringUtf8(), onRef, contentId);
    }
    if (type.equals(WithAttachmentsContent.WITH_ATTACHMENTS)) {
      Stream<ContentAttachmentKey> keys = Stream.empty();
      try (Stream<ContentAttachment> attachments = attachmentsRetriever.apply(keys)) {
        assertThat(attachments).isNotEmpty();
        return withAttachments(attachments.collect(Collectors.toList()), onRef, contentId);
      }
    }
    throw new IllegalArgumentException("" + onReferenceValue);
  }

  @Override
  public Content applyId(Content content, String id) {
    Objects.requireNonNull(content, "Content must not be null");
    Objects.requireNonNull(id, "id must not be null");
    if (content instanceof OnRefOnly) {
      OnRefOnly onRef = (OnRefOnly) content;
      return OnRefOnly.onRef(onRef.getOnRef(), id);
    }
    if (content instanceof WithGlobalStateContent) {
      WithGlobalStateContent withGlobal = (WithGlobalStateContent) content;
      return WithGlobalStateContent.withGlobal(withGlobal.getGlobal(), withGlobal.getOnRef(), id);
    }
    throw new IllegalArgumentException("Unknown type " + content);
  }

  @Override
  public Content.Type getType(ByteString onRefContent) {
    String serialized = onRefContent.toStringUtf8();
    int i = serialized.indexOf(':');
    if (i == -1) {
      return OnRefOnly.ON_REF_ONLY;
    }
    String typeString = serialized.substring(0, i);
    return ContentTypes.forName(typeString);
  }

  @Override
  public boolean requiresGlobalState(ByteString content) {
    return getType(content) == WithGlobalStateContent.WITH_GLOBAL_STATE;
  }

  @Override
  public boolean requiresGlobalState(Content content) {
    return content instanceof WithGlobalStateContent;
  }
}
