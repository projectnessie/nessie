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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.projectnessie.versioned.storage.common.persist.Backend;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.PersistFactory;
import org.projectnessie.versioned.storage.common.persist.Reference;

public final class InmemoryBackend implements Backend {
  final Map<String, Reference> references = new ConcurrentHashMap<>();
  final Map<String, Obj> objects = new ConcurrentHashMap<>();

  @Override
  @Nonnull
  @jakarta.annotation.Nonnull
  public PersistFactory createFactory() {
    return new InmemoryPersistFactory(this);
  }

  @Override
  public void close() {
    references.clear();
    objects.clear();
  }

  @Override
  public void setupSchema() {}

  @Override
  public String configInfo() {
    return "";
  }
}
