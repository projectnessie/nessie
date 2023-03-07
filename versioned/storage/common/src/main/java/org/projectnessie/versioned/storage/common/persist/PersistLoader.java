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
package org.projectnessie.versioned.storage.common.persist;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Predicate;

public final class PersistLoader {
  private PersistLoader() {}

  public static <C> BackendFactory<C> findFactoryByName(String name) {
    return findFactory(f -> f.name().equals(name));
  }

  public static <C> BackendFactory<C> findAny() {
    return findFactory(x -> true);
  }

  public static <C> BackendFactory<C> findFactory(Predicate<BackendFactory<?>> filter) {
    ServiceLoader<BackendFactory<C>> loader = loader();
    List<BackendFactory<?>> candidates = new ArrayList<>();
    boolean any = false;
    for (BackendFactory<C> backendFactory : loader) {
      any = true;
      if (filter.test(backendFactory)) {
        candidates.add(backendFactory);
      }
    }
    checkState(any, "No BackendFactory on class path");
    checkArgument(!candidates.isEmpty(), "No BackendFactory matched the given filter");
    checkState(candidates.size() == 1, "More than one BackendFactory matched the given filter");

    return cast(candidates.get(0));
  }

  // Helper for ugly generics casting
  private static <C> ServiceLoader<BackendFactory<C>> loader() {
    @SuppressWarnings("rawtypes")
    ServiceLoader<BackendFactory> f = ServiceLoader.load(BackendFactory.class);
    @SuppressWarnings({"unchecked", "rawtypes"})
    ServiceLoader<BackendFactory<C>> r = (ServiceLoader) f;
    return r;
  }

  // Helper for ugly generics casting
  private static <C> BackendFactory<C> cast(BackendFactory<?> backendFactory) {
    @SuppressWarnings("unchecked")
    BackendFactory<C> r = (BackendFactory<C>) backendFactory;
    return r;
  }
}
