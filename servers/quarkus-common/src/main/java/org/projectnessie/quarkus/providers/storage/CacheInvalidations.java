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
package org.projectnessie.quarkus.providers.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(jdkOnly = true)
@JsonSerialize(as = ImmutableCacheInvalidations.class)
@JsonDeserialize(as = ImmutableCacheInvalidations.class)
public interface CacheInvalidations {
  @Value.Parameter(order = 1)
  List<CacheInvalidation> invalidations();

  static CacheInvalidations cacheInvalidations(List<CacheInvalidation> invalidations) {
    return ImmutableCacheInvalidations.of(invalidations);
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "t")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = CacheInvalidationPutObj.class, name = CacheInvalidationPutObj.TYPE),
    @JsonSubTypes.Type(
        value = CacheInvalidationRemoveObj.class,
        name = CacheInvalidationRemoveObj.TYPE),
    @JsonSubTypes.Type(
        value = CacheInvalidationPutReference.class,
        name = CacheInvalidationPutReference.TYPE),
    @JsonSubTypes.Type(
        value = CacheInvalidationRemoveReference.class,
        name = CacheInvalidationRemoveReference.TYPE),
  })
  interface CacheInvalidation {
    String type();

    @JsonProperty("r")
    String repoId();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationPutObj.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationPutObj.class)
  @JsonTypeName(value = CacheInvalidationPutObj.TYPE)
  interface CacheInvalidationPutObj extends CacheInvalidation {
    String TYPE = "obj";

    @Override
    default String type() {
      return TYPE;
    }

    @Value.Parameter(order = 1)
    @Override
    String repoId();

    @Value.Parameter(order = 2)
    byte[] id();

    @Value.Parameter(order = 3)
    @JsonProperty("h")
    int hash();

    static CacheInvalidationPutObj cacheInvalidationPutObj(String repoId, byte[] id, int hash) {
      return ImmutableCacheInvalidationPutObj.of(repoId, id, hash);
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationRemoveObj.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationRemoveObj.class)
  @JsonTypeName(value = CacheInvalidationRemoveObj.TYPE)
  interface CacheInvalidationRemoveObj extends CacheInvalidation {
    String TYPE = "rmObj";

    @Override
    default String type() {
      return TYPE;
    }

    @Value.Parameter(order = 1)
    @Override
    String repoId();

    @Value.Parameter(order = 2)
    byte[] id();

    static CacheInvalidationRemoveObj cacheInvalidationRemoveObj(String repoId, byte[] id) {
      return ImmutableCacheInvalidationRemoveObj.of(repoId, id);
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationPutReference.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationPutReference.class)
  @JsonTypeName(value = CacheInvalidationPutReference.TYPE)
  interface CacheInvalidationPutReference extends CacheInvalidation {
    String TYPE = "ref";

    @Override
    default String type() {
      return TYPE;
    }

    @Value.Parameter(order = 1)
    @Override
    String repoId();

    @Value.Parameter(order = 2)
    @JsonProperty("ref")
    String refName();

    @Value.Parameter(order = 3)
    @JsonProperty("h")
    int hash();

    static CacheInvalidationPutReference cacheInvalidationPutReference(
        String repoId, String refName, int hash) {
      return ImmutableCacheInvalidationPutReference.of(repoId, refName, hash);
    }
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableCacheInvalidationRemoveReference.class)
  @JsonDeserialize(as = ImmutableCacheInvalidationRemoveReference.class)
  @JsonTypeName(value = CacheInvalidationRemoveReference.TYPE)
  interface CacheInvalidationRemoveReference extends CacheInvalidation {
    String TYPE = "rmRef";

    @Override
    default String type() {
      return TYPE;
    }

    @Value.Parameter(order = 1)
    @Override
    String repoId();

    @Value.Parameter(order = 2)
    @JsonProperty("ref")
    String refName();

    static CacheInvalidationRemoveReference cacheInvalidationRemoveReference(
        String repoId, String refName) {
      return ImmutableCacheInvalidationRemoveReference.of(repoId, refName);
    }
  }
}
