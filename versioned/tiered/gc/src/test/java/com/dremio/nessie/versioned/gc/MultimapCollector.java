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
package com.dremio.nessie.versioned.gc;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * Collector to collect into multimap.
 */
class MultimapCollector<T, K, V> implements Collector<T, Multimap<K, V>, Multimap<K, V>> {

  private final Function<T, K> keyGetter;
  private final Function<T, V> valueGetter;

  public MultimapCollector(Function<T, K> keyGetter, Function<T, V> valueGetter) {
    this.keyGetter = keyGetter;
    this.valueGetter = valueGetter;
  }

  public static <T, K, V> MultimapCollector<T, K, V> toMultimap(Function<T, K> keyGetter, Function<T, V> valueGetter) {
    return new MultimapCollector<>(keyGetter, valueGetter);
  }

  @Override
  public Supplier<Multimap<K, V>> supplier() {
    return ArrayListMultimap::create;
  }

  @Override
  public BiConsumer<Multimap<K, V>, T> accumulator() {
    return (map, element) -> map.put(keyGetter.apply(element), valueGetter.apply(element));
  }

  @Override
  public BinaryOperator<Multimap<K, V>> combiner() {
    return (map1, map2) -> {
      map1.putAll(map2);
      return map1;
    };
  }

  @Override
  public Function<Multimap<K, V>, Multimap<K, V>> finisher() {
    return map -> map;
  }

  @Override
  public Set<Characteristics> characteristics() {
    return ImmutableSet.of(Characteristics.IDENTITY_FINISH);
  }
}
