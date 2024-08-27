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

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.projectnessie.model.Content;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.StoreWorker;

/**
 * Main {@link StoreWorker} implementation that maintains a registry of available {@link
 * org.projectnessie.versioned.store.ContentSerializer content serializers} and delegates to these.
 */
public class DefaultStoreWorker implements StoreWorker {

  public static StoreWorker instance() {
    return Lazy.INSTANCE;
  }

  public static int payloadForContent(Content c) {
    return serializer(c).payload();
  }

  public static int payloadForContent(Content.Type contentType) {
    return serializer(contentType).payload();
  }

  public static Content.Type contentTypeForPayload(int payload) {
    Content.Type contentType =
        payload >= 0 && payload < Registry.BY_PAYLOAD.length
            ? Registry.BY_PAYLOAD[payload].contentType()
            : null;
    if (contentType == null) {
      throw new IllegalArgumentException("Unknown payload " + payload);
    }
    return contentType;
  }

  private static final class Lazy {
    private static final DefaultStoreWorker INSTANCE = new DefaultStoreWorker();
  }

  private static final class Registry {
    private static final ContentSerializer<?>[] BY_PAYLOAD;
    private static final Map<Content.Type, ContentSerializer<?>> BY_TYPE;

    static {
      Map<String, ContentSerializer<?>> byName = new HashMap<>();
      Map<Content.Type, ContentSerializer<?>> byType = new HashMap<>();
      List<ContentSerializer<?>> byPayload = new ArrayList<>();

      for (ContentSerializerBundle bundle : ServiceLoader.load(ContentSerializerBundle.class)) {
        bundle.register(
            contentTypeSerializer -> {
              Content.Type contentType = contentTypeSerializer.contentType();
              if (byName.put(contentType.name(), contentTypeSerializer) != null) {
                throw new IllegalStateException(
                    "Found more than one ContentTypeSerializer for name " + contentType.name());
              }
              if (contentTypeSerializer.payload() != 0
                  && byType.put(contentType, contentTypeSerializer) != null) {
                throw new IllegalStateException(
                    "Found more than one ContentTypeSerializer for content type "
                        + contentType.type());
              }
              while (byPayload.size() <= contentTypeSerializer.payload()) {
                byPayload.add(null);
              }
              if (byPayload.set(contentTypeSerializer.payload(), contentTypeSerializer) != null) {
                throw new IllegalStateException(
                    "Found more than one ContentTypeSerializer for content payload "
                        + contentTypeSerializer.payload());
              }
            });
      }

      BY_PAYLOAD = byPayload.toArray(new ContentSerializer[0]);
      BY_TYPE = byType;
    }
  }

  private static @Nonnull <C extends Content> ContentSerializer<C> serializer(C content) {
    return serializer(content.getType());
  }

  private static @Nonnull <C extends Content> ContentSerializer<C> serializer(
      Content.Type contentType) {
    @SuppressWarnings("unchecked")
    ContentSerializer<C> serializer = (ContentSerializer<C>) Registry.BY_TYPE.get(contentType);
    if (serializer == null) {
      throw new IllegalArgumentException("No type registered for " + contentType);
    }
    return serializer;
  }

  private static @Nonnull <C extends Content> ContentSerializer<C> serializer(int payload) {
    @SuppressWarnings("unchecked")
    ContentSerializer<C> serializer =
        payload >= 0 && payload < Registry.BY_PAYLOAD.length
            ? (ContentSerializer<C>) Registry.BY_PAYLOAD[payload]
            : null;
    if (serializer == null) {
      throw new IllegalArgumentException("Unknown payload " + payload);
    }
    return serializer;
  }

  @Override
  public ByteString toStoreOnReferenceState(Content content) {
    return serializer(content).toStoreOnReferenceState(content);
  }

  @Override
  public Content valueFromStore(int payload, ByteString onReferenceValue) {
    ContentSerializer<Content> serializer = serializer(payload);
    return serializer.valueFromStore(onReferenceValue);
  }
}
