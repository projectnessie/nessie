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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;

/** A set of helpers that users of a VersionStore must implement. */
public interface StoreWorker {

  /** Returns the serialized representation of the on-reference part of the given content-object. */
  ByteString toStoreOnReferenceState(
      Content content, Consumer<ContentAttachment> attachmentConsumer);

  ByteString toStoreGlobalState(Content content);

  Content valueFromStore(
      ByteString onReferenceValue,
      Supplier<ByteString> globalState,
      Function<Stream<ContentAttachmentKey>, Stream<ContentAttachment>> attachmentsRetriever);

  Content applyId(Content content, String id);

  String getId(Content content);

  Byte getPayload(Content content);

  boolean requiresGlobalState(ByteString content);

  boolean requiresGlobalState(Content content);

  Content.Type getType(ByteString onRefContent);

  Content.Type getType(Byte payload);

  default Content.Type getType(Content content) {
    return getType(getPayload(content));
  }

  Serializer<CommitMeta> getMetadataSerializer();

  default boolean isNamespace(ByteString type) {
    return false;
  }
}
