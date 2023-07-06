/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.api.impl;

import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * A helper that implements {@link Map.Entry}, because the code is built for Java 8 and Guava's not
 * a dependency.
 */
@Value.Immutable
abstract class MapEntry<K, V> implements Map.Entry<K, V> {
  private static final MapEntry<?, ?> EMPTY_ENTRY = (MapEntry<?, ?>) MapEntry.mapEntry(null, null);

  @SuppressWarnings("unchecked")
  static <K, V> Map.Entry<K, V> emptyEntry() {
    return (Map.Entry<K, V>) EMPTY_ENTRY;
  }

  static <K, V> Map.Entry<K, V> mapEntry(K key, V value) {
    return ImmutableMapEntry.of(key, value);
  }

  @Override
  @Value.Parameter(order = 1)
  @Nullable
  public abstract K getKey();

  @Override
  @Value.Parameter(order = 2)
  @Nullable
  public abstract V getValue();

  @Override
  public V setValue(V value) {
    throw new UnsupportedOperationException();
  }
}
