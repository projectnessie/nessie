/*
 * Copyright (C) 2024 Dremio
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

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;
import org.projectnessie.model.ContentKey;

/** Additional information related to the incoming API request. */
@Value.Immutable
@Value.Style(allParameters = true)
public interface RequestMeta {
  /** Indicates whether access checks shall be performed for a write/update request. */
  boolean forWrite();

  @Value.Default
  default Map<ContentKey, Set<String>> keyActions() {
    return Map.of();
  }

  default Set<String> keyActions(ContentKey key) {
    return keyActions().getOrDefault(key, Set.of());
  }

  static RequestMetaBuilder apiWrite() {
    return new RequestMetaBuilder().forWrite(true);
  }

  static RequestMetaBuilder apiRead() {
    return new RequestMetaBuilder().forWrite(false);
  }

  RequestMeta API_WRITE = apiWrite().build();
  RequestMeta API_READ = apiRead().build();

  final class RequestMetaBuilder {
    private final Map<ContentKey, Set<String>> keyActions = new HashMap<>();
    private boolean forWrite;

    public RequestMetaBuilder forWrite(boolean forWrite) {
      this.forWrite = forWrite;
      return this;
    }

    public RequestMetaBuilder addKeyAction(ContentKey key, String name) {
      keyActions.computeIfAbsent(key, x -> new HashSet<>()).add(name);
      return this;
    }

    public RequestMetaBuilder addKeyActions(ContentKey key, Set<String> names) {
      keyActions.computeIfAbsent(key, x -> new HashSet<>()).addAll(names);
      return this;
    }

    public RequestMetaBuilder newBuilder() {
      RequestMetaBuilder newBuilder = new RequestMetaBuilder().forWrite(forWrite);
      keyActions.forEach(newBuilder::addKeyActions);
      return newBuilder;
    }

    public RequestMeta build() {
      var immutableKeyActions = ImmutableMap.<ContentKey, Set<String>>builder();
      keyActions.forEach((k, v) -> immutableKeyActions.put(k, Set.copyOf(v)));
      return ImmutableRequestMeta.of(forWrite, immutableKeyActions.build());
    }
  }
}
