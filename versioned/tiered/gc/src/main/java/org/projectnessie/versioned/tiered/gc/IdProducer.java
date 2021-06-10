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
package org.projectnessie.versioned.tiered.gc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.gc.BinaryBloomFilter;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.Store.Acceptor;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;
import org.sparkproject.guava.collect.Iterators;

/**
 * Convert an item to valid version of it's children.
 *
 * <p>Given a particular item and a bloomfilter listing valid ids, determine if the current item is
 * valid. If the current item is valid, generate all the referenced children ids.
 */
final class IdProducer {

  /**
   * Given a bloom filter and list of items that can produce addition Ids, get the next bloom
   * filter.
   */
  public static <T extends BaseValue<T>> BinaryBloomFilter getNextBloomFilter(
      SparkSession spark,
      BinaryBloomFilter idFilter,
      ValueType<T> valueType,
      Supplier<Store> store,
      long targetCount,
      Function<Acceptor<T>, IdCarrier> converter) {

    Predicate<IdCarrier> predicate = t -> idFilter.mightContain(t.getId().getId());
    Dataset<IdCarrier> carriers =
        IdCarrier.asDataset(valueType, store, converter, Optional.of(predicate), spark);
    return BinaryBloomFilter.aggregate(
        carriers.withColumn("id", explode(col("children"))), "id.id");
  }

  /**
   * Given a bloom filter and list of items that can produce addition Ids, get the next bloom
   * filter.
   */
  public static <T extends BaseValue<T>> Dataset<IdKeyPair> getKeys(
      SparkSession spark,
      ValueType<T> valueType,
      Supplier<Store> store,
      Function<Acceptor<T>, IdCarrier> converter) {

    Dataset<IdCarrier> carriers =
        IdCarrier.asDataset(valueType, store, converter, Optional.empty(), spark);
    return carriers.flatMap(new KeyPairFlatMap(), Encoders.bean(IdKeyPair.class)).distinct();
  }

  public static class KeyPairFlatMap implements FlatMapFunction<IdCarrier, IdKeyPair> {

    @Override
    public Iterator<IdKeyPair> call(IdCarrier idCarrier) throws Exception {
      if (idCarrier.getChildKeys() == null || idCarrier.getChildKeys().isEmpty()) {
        return Iterators.emptyIterator();
      }
      return idCarrier.getChildKeys().entrySet().stream().map(IdKeyPair::of).iterator();
    }
  }

  public static class IdKeyPair implements Serializable {
    private IdFrame id;
    private List<String> key;

    public IdKeyPair() {}

    public IdKeyPair(IdFrame id, List<String> key) {
      this.id = id;
      this.key = key;
    }

    public static IdKeyPair of(Map.Entry<IdFrame, List<String>> kv) {
      return new IdKeyPair(kv.getKey(), kv.getValue());
    }

    public IdFrame getId() {
      return id;
    }

    public void setId(IdFrame id) {
      this.id = id;
    }

    public List<String> getKey() {
      return key;
    }

    public void setKey(List<String> key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      IdKeyPair idKeyPair = (IdKeyPair) o;
      return id.equals(idKeyPair.id);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }
  }
}
