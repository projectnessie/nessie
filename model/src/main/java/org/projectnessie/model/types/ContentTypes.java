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
package org.projectnessie.model.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import org.projectnessie.model.Content;

/**
 * Provides the registry for all {@link Content content types}.
 *
 * <p>Content types are loaded via {@link ContentTypeBundle}s using Java's {@link ServiceLoader
 * service loader} mechanism.
 */
public final class ContentTypes {

  /**
   * An implementation of this interface is passed to {@link
   * org.projectnessie.model.types.ContentTypeBundle}s.
   */
  public interface Register {
    void register(String name, byte payload, Class<? extends Content> type);
  }

  /** Retrieve an array of all registered content types. */
  public static Content.Type[] all() {
    return Registry.all();
  }

  /** Retrieve the content type for a payload value. */
  public static Content.Type forPayload(byte payload) {
    return Registry.forPayload(payload);
  }

  /** Retrieve the content-type for a name. */
  public static Content.Type forName(String name) {
    return Registry.forName(name);
  }

  /**
   * Internal class providing the actual registry. This is a separate class to implicitly use lazy
   * initialization.
   */
  private static final class Registry {

    private static final Content.Type[] all;
    private static final Content.Type[] payloads;
    private static final Map<String, Content.Type> byName;

    static {
      List<Content.Type> list = new ArrayList<>();
      Map<String, Content.Type> names = new HashMap<>();

      // Add the "DEFAULT" type.
      Content.Type unknownContentType = new DefaultContentTypeImpl();
      list.add(unknownContentType);
      names.put(unknownContentType.name(), unknownContentType);

      for (ContentTypeBundle bundle : ServiceLoader.load(ContentTypeBundle.class)) {
        bundle.register(
            (name, payload, type) -> {
              if (payload <= (byte) 0
                  || name == null
                  || name.trim().isEmpty()
                  || !name.trim().equals(name)
                  || type == null) {
                throw new IllegalArgumentException(
                    String.format(
                        "Illegal content-type registration: payload=%d, name=%s, type=%s",
                        payload, name, type));
              }
              Content.Type contentType = new ContentTypeImpl(name, payload, type);
              while (list.size() <= payload) {
                list.add(null);
              }
              Content.Type ex = list.get(payload);
              if (ex == null) {
                ex = names.get(name);
              }
              if (ex != null) {
                throw new IllegalStateException(
                    String.format(
                        "Duplicate content type registration for %d/%s/%s, existing: %d/%s/%s",
                        payload, name, type, ex.payload(), ex.name(), ex.type()));
              }
              list.set(payload, contentType);
              names.put(name, contentType);
            });
      }

      payloads = list.toArray(new Content.Type[0]);
      byName = Collections.unmodifiableMap(names);
      all = list.stream().filter(Objects::nonNull).toArray(Content.Type[]::new);
    }

    private static Content.Type[] all() {
      return all.clone();
    }

    private static Content.Type forPayload(int payload) {
      if (payload < 0) {
        throw new IllegalArgumentException("Illegal payload " + payload + ", must be positive.");
      }
      return Objects.requireNonNull(
          payload > payloads.length ? null : payloads[payload],
          "No content type registered for payload " + payload);
    }

    private static Content.Type forName(String name) {
      return Objects.requireNonNull(
          byName.get(name), "No content type registered for name " + name);
    }
  }

  /** Internally used wrapper for a {@link Content.Type}. */
  private static final class ContentTypeImpl implements Content.Type {

    private final String name;
    private final byte payload;
    private final Class<? extends Content> type;

    private ContentTypeImpl(String name, byte payload, Class<? extends Content> type) {
      this.name = name;
      this.payload = payload;
      this.type = type;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public byte payload() {
      return payload;
    }

    @Override
    public Class<? extends Content> type() {
      return type;
    }

    @Override
    public String toString() {
      return name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ContentTypeImpl)) {
        return false;
      }
      ContentTypeImpl that = (ContentTypeImpl) o;
      return payload == that.payload;
    }

    @Override
    public int hashCode() {
      return payload;
    }
  }

  /** Internally used wrapper for the {@code UNKNOWN} content type. */
  private static final class DefaultContentTypeImpl implements Content.Type {
    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public String toString() {
      return name();
    }

    @Override
    public String name() {
      return "UNKNOWN";
    }

    @Override
    public byte payload() {
      return 0;
    }

    @Override
    public Class<? extends Content> type() {
      throw new IllegalStateException("UNKNOWN Content.Type has no type");
    }
  }
}
