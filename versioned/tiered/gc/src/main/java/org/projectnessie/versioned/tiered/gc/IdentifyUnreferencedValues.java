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

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.gc.BinaryBloomFilter;
import org.projectnessie.versioned.gc.CategorizedValue;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import scala.Tuple2;

/** Operation which identifies the referenced state of all values in a tiered version store. */
public class IdentifyUnreferencedValues<T> {

  private final StoreWorker<T, ?, ?> storeWorker;
  private final Supplier<Store> store;
  private final SparkSession spark;
  private final GcOptions options;
  private final Clock clock;

  /** Drive a job that generates a dataset of unreferenced assets from known values. */
  public IdentifyUnreferencedValues(
      StoreWorker<T, ?, ?> storeWorker,
      Supplier<Store> store,
      SparkSession spark,
      GcOptions options,
      Clock clock) {
    super();
    this.storeWorker = storeWorker;
    this.store = store;
    this.spark = spark;
    this.options = options;
    this.clock = clock;
  }

  public Dataset<CategorizedValue> identify() throws AnalysisException {
    return go(storeWorker, store, spark, options, clock);
  }

  private static <T> Dataset<CategorizedValue> go(
      StoreWorker<T, ?, ?> storeWorker,
      Supplier<Store> store,
      SparkSession spark,
      GcOptions options,
      Clock clock)
      throws AnalysisException {

    // fix all times to be at the same start point. todo push up to GcOptions and remove System call
    long now = clock.millis();
    long maxAgeMicros = TimeUnit.MILLISECONDS.toMicros(now) - options.getMaxAgeMicros();
    long maxSlopMicros = TimeUnit.MILLISECONDS.toMicros(now) - options.getTimeSlopMicros();

    // get bloomfilter of valid l2s (based on the given gc policy).
    final BinaryBloomFilter l2BloomFilter =
        RefToL2Producer.getL2BloomFilter(
            spark, store, maxAgeMicros, maxSlopMicros, options.getBloomFilterCapacity());

    // get bloomfilter of valid l3s.
    final BinaryBloomFilter l3BloomFilter =
        IdProducer.getNextBloomFilter(
            spark,
            l2BloomFilter,
            ValueType.L2,
            store,
            options.getBloomFilterCapacity(),
            IdCarrier.L2_CONVERTER);

    // get a bloom filter of all values that are referenced by a valid value.
    final BinaryBloomFilter validValueIds =
        IdProducer.getNextBloomFilter(
            spark,
            l3BloomFilter,
            ValueType.L3,
            store,
            options.getBloomFilterCapacity(),
            IdCarrier.L3_CONVERTER);
    Dataset<IdProducer.IdKeyPair> keys =
        IdProducer.getKeys(spark, ValueType.L3, store, IdCarrier.L3_CONVERTER);

    // get all values.
    final Dataset<ValueFrame> values = ValueFrame.asDataset(store, spark);
    Dataset<Tuple2<ValueFrame, IdProducer.IdKeyPair>> valuesWithKeys =
        values.joinWith(keys, values.col("id").equalTo(keys.col("id")), "left");

    // for each value, determine if it is a valid value.
    ValueCategorizer categorizer = new ValueCategorizer(validValueIds, maxSlopMicros);
    // todo can we enrich this more w/o having to interpret the object? Eg related commit/commit
    // message
    return valuesWithKeys.map(categorizer, Encoders.bean(CategorizedValue.class));
  }

  /**
   * Spark flat map function to convert a value into an iterator of AssetKeys with their reference
   * state.
   */
  public static class ValueCategorizer
      implements MapFunction<Tuple2<ValueFrame, IdProducer.IdKeyPair>, CategorizedValue> {

    private static final long serialVersionUID = -4605489080345105845L;

    private final BinaryBloomFilter bloomFilter;
    private final long recentValues;

    /**
     * Construct categorizer.
     *
     * @param bloomFilter bloom filter to determine if a value is referenced.
     * @param maxSlopMicros minimum age of a value to consider it as being unreferenced.
     */
    public ValueCategorizer(BinaryBloomFilter bloomFilter, long maxSlopMicros) {
      this.bloomFilter = bloomFilter;
      this.recentValues = maxSlopMicros;
    }

    @Override
    public CategorizedValue call(Tuple2<ValueFrame, IdProducer.IdKeyPair> pair) throws Exception {
      ValueFrame r = pair._1;
      List<String> key = pair._2 == null ? Collections.emptyList() : pair._2.getKey();
      boolean referenced = r.getDt() > recentValues || bloomFilter.mightContain(r.getId().getId());
      return new CategorizedValue(referenced, ByteString.copyFrom(r.getBytes()), r.getDt(), key);
    }
  }
}
