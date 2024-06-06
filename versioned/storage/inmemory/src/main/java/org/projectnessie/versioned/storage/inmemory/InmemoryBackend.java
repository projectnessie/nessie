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
package org.projectnessie.versioned.storage.inmemory;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.Reference;

public final class InmemoryBackend implements Backend {
  final Map<String, Reference> references = new ConcurrentHashMap<>();
  final Map<String, Obj> objects = new ConcurrentHashMap<>();

  static String compositeKeyRepo(String repoId) {
    return repoId + ':';
  }

  @Override
  @Nonnull
  public PersistFactory createFactory() {
    return new InmemoryPersistFactory(this);
  }

  @Override
  public void close() {
    references.clear();
    objects.clear();
  }

  @Override
  public Optional<String> setupSchema() {
    return Optional.empty();
  }

  @Override
  public void eraseRepositories(Set<String> repositoryIds) {
    if (repositoryIds == null || repositoryIds.isEmpty()) {
      return;
    }

    List<String> prefixed =
        repositoryIds.stream().map(InmemoryBackend::compositeKeyRepo).collect(Collectors.toList());

    Consumer<Map<String, ?>> cleaner =
        m -> m.keySet().removeIf(k -> prefixed.stream().anyMatch(k::startsWith));

    cleaner.accept(references);
    cleaner.accept(objects);
  }
}
