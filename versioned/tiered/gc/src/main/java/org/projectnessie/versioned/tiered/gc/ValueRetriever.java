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

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

/** Utility class used to extract values from underlying store and convert to a Spark Dataset. */
public class ValueRetriever<T extends BaseValue<T>, OUT> {

  private final Supplier<Store> supplier;
  private final ValueType<T> valueType;
  private final Function<Store.Acceptor<T>, OUT> builder;
  private final Class<OUT> clazz;

  /** Construct a retriever. */
  public ValueRetriever(
      Supplier<Store> supplier,
      ValueType<T> valueType,
      Function<Store.Acceptor<T>, OUT> builder,
      Class<OUT> clazz) {
    super();
    this.supplier = supplier;
    this.valueType = valueType;
    this.builder = builder;
    this.clazz = clazz;
  }

  private Stream<OUT> stream() {
    return supplier.get().getValues(valueType).map(builder::apply);
  }

  /** Get a dataset from this retriever. */
  public Dataset<OUT> get(SparkSession spark, Optional<Predicate<OUT>> filter) {
    List<OUT> out = this.stream().filter(filter.orElse(t -> true)).collect(Collectors.toList());
    return spark.createDataset(out, Encoders.bean(clazz));
  }

  public static <IN extends BaseValue<IN>, OUT> Dataset<OUT> dataset(
      Supplier<Store> supplier,
      ValueType<IN> valueType,
      Class<OUT> clazz,
      Optional<Predicate<OUT>> filter,
      SparkSession spark,
      Function<Store.Acceptor<IN>, OUT> converter) {
    return new ValueRetriever<IN, OUT>(supplier, valueType, converter, clazz).get(spark, filter);
  }
}
