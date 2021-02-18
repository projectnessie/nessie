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

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.gc.core.BinaryBloomFilter;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.Store.Acceptor;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

/**
 * Convert an item to valid version of it's children.
 *
 * <p>Given a particular item and a bloomfilter listing valid ids, determine if the current item is valid.
 * If the current item is valid, generate all the referenced children ids.
 */
final class IdProducer {

  /**
   * Given a bloom filter and list of items that can produce addition Ids, get the next bloom filter.
   */
  public static <T extends BaseValue<T>> BinaryBloomFilter getNextBloomFilter(
      SparkSession spark,
      BinaryBloomFilter idFilter,
      ValueType<T> valueType,
      Supplier<Store> store,
      long targetCount,
      Function<Acceptor<T>, IdCarrier> converter) {

    Predicate<IdCarrier> predicate = t -> idFilter.mightContain(t.getId());
    Dataset<IdCarrier> carriers = IdCarrier.asDataset(valueType, store, converter, Optional.of(predicate), spark);
    return BinaryBloomFilter.aggregate(carriers.withColumn("id", explode(col("children"))), "id.id");

  }

}
