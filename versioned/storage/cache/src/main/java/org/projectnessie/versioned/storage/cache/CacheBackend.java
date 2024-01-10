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
package org.projectnessie.versioned.storage.cache;

import jakarta.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Provides the cache primitives for a caching {@link Persist} facade, suitable for multiple
 * repositories. It is adviseable to have one {@link CacheBackend} per {@link Backend}.
 */
public interface CacheBackend {
  Obj get(@Nonnull String repositoryId, @Nonnull ObjId id);

  void put(@Nonnull String repositoryId, @Nonnull Obj obj);

  void remove(@Nonnull String repositoryId, @Nonnull ObjId id);

  void clear(@Nonnull String repositoryId);

  Persist wrap(@Nonnull Persist perist);
}
