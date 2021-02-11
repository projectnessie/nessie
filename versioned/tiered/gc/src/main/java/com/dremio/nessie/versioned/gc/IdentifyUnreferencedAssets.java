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

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.dremio.nessie.versioned.AssetKey;
import com.dremio.nessie.versioned.Serializer;
import com.dremio.nessie.versioned.StoreWorker;
import com.dremio.nessie.versioned.ValueWorker;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import scala.Function1;

/**
 * Operation which identifies unreferenced assets.
 */
public class IdentifyUnreferencedAssets<T> {

  private final StoreWorker<T, ?> storeWorker;
  private final Supplier<Store> store;
  private final SparkSession spark;
  private final GcOptions options;

  /**
   * Drive a job that generates a dataset of unreferenced assets.
   */
  public IdentifyUnreferencedAssets(
      StoreWorker<T, ?> storeWorker,
      Supplier<Store> store,
      SparkSession spark,
      GcOptions options) {
    super();
    this.storeWorker = storeWorker;
    this.store = store;
    this.spark = spark;
    this.options = options;
  }

  public Dataset<UnreferencedItem> identify() throws AnalysisException {
    return go(storeWorker, store, spark, options);
  }

  private static <T> Dataset<UnreferencedItem> go(
      StoreWorker<T, ?> storeWorker,
      Supplier<Store> store,
      SparkSession spark,
      GcOptions options) throws AnalysisException {

    final ValueWorker<T> valueWorker = storeWorker.getValueWorker();

    // get bloomfilter of valid l2s (based on the given gc policy).
    final BinaryBloomFilter l2BloomFilter = RefToL2Producer
        .getL2BloomFilter(spark, store, options.getMaxAgeMicros(), options.getTimeSlopMicros(), options.getBloomFilterCapacity());

    // get bloomfilter of valid l3s.
    final BinaryBloomFilter l3BloomFilter = IdProducer
        .getNextBloomFilter(spark, l2BloomFilter, ValueType.L2, store, options.getBloomFilterCapacity(), IdCarrier.L2_CONVERTER);

    // get a bloom filter of all values that are referenced by a valid value.
    final BinaryBloomFilter validValueIds = IdProducer
        .getNextBloomFilter(spark, l3BloomFilter, ValueType.L3, store, options.getBloomFilterCapacity(), IdCarrier.L3_CONVERTER);

    // get all values.
    final Dataset<ValueFrame> values = ValueFrame.asDataset(store, spark);

    // for each value, determine if it is a valid value. If it is, generate a referenced asset. If not, generate a non-referenced asset.
    // this is a single output that has a categorization column
    AssetCategorizer<T> categorizer = new AssetCategorizer<T>(validValueIds, storeWorker.getValueWorker(), options.getTimeSlopMicros());
    Dataset<CategorizedAssetKey> assets = values.flatMap(categorizer, Encoders.bean(CategorizedAssetKey.class));

    // generate a bloom filter of referenced items.
    final BinaryBloomFilter referencedAssets = BinaryBloomFilter.aggregate(assets.filter("referenced = true").select("data"), "data");

    // generate list of maybe referenced assets (note that a single asset may be referenced by both referenced and non-referenced values).
    // TODO: convert this to a group by asset, date and then figure out the latest date so we can include that
    // in the written file to avoid stale -> not stale -> stale values.
    Dataset<Row> unreferencedAssets = assets.filter("referenced = false").select("data").filter(new AssetFilter(referencedAssets));

    // map the generic spark Row back to a concrete type.
    return unreferencedAssets.map(
        new UnreferencedItemConverter(valueWorker.getAssetKeySerializer()), Encoders.bean(UnreferencedItem.class));
  }

  /**
   * Spark filter to determine if a value is referenced by checking if the byte[] serialization is in a bloom filter.
   */
  public static class AssetFilter implements FilterFunction<Row> {

    private static final long serialVersionUID = 2411246084016802962L;

    private BinaryBloomFilter filter;

    public AssetFilter() {
    }

    public AssetFilter(BinaryBloomFilter filter) {
      this.filter = filter;
    }

    @Override
    public boolean call(Row r) throws Exception {
      byte[] bytes = r.getAs("data");
      return !filter.mightContain(bytes);
    }

  }

  /**
   * Pair of referenced state of an asset key and its byte[] representation.
   */
  public static final class CategorizedAssetKey implements Serializable {

    private static final long serialVersionUID = -1466847843373432962L;

    private boolean referenced;
    private byte[] data;

    public CategorizedAssetKey() {
    }

    /**
     * Construct asset key.
     */
    public CategorizedAssetKey(boolean referenced, ByteString data) {
      super();
      this.referenced = referenced;
      this.data = data.toByteArray();
    }

    public void setReferenced(boolean referenced) {
      this.referenced = referenced;
    }

    public void setData(byte[] data) {
      this.data = data;
    }

    public boolean isReferenced() {
      return referenced;
    }

    public byte[] getData() {
      return data;
    }

  }

  /**
   * Spark function to convert a Row into a concrete UnreferencedItem object.
   */
  public static class UnreferencedItemConverter implements Function1<Row, UnreferencedItem>, Serializable {
    private static final long serialVersionUID = -5135625090051205329L;

    private final Serializer<AssetKey> serializer;

    public UnreferencedItemConverter(Serializer<AssetKey> serializer) {
      this.serializer = serializer;
    }

    @Override
    public UnreferencedItem apply(Row r) {
      byte[] asset = r.getAs("data");
      AssetKey key = serializer.fromBytes(UnsafeByteOperations.unsafeWrap(asset));
      UnreferencedItem ui = new UnreferencedItem();
      ui.setName(key.toReportableName().stream().collect(Collectors.joining(".")));
      ui.setAsset(asset);
      return ui;
    }

  }

  /**
   * Unreferenced Item. Pair of name of unreferenced item and the byte[] representation of the underlying AssetKey.
   */
  public static class UnreferencedItem implements Serializable {
    private static final long serialVersionUID = -5566256066143995534L;

    private String name;
    private byte[] asset;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public byte[] getAsset() {
      return asset;
    }

    public void setAsset(byte[] asset) {
      this.asset = asset;
    }

  }

  /**
   * Spark flat map function to convert a value into an iterator of AssetKeys with their reference state.
   */
  public static class AssetCategorizer<T> implements FlatMapFunction<ValueFrame, CategorizedAssetKey> {

    private static final long serialVersionUID = -4605489080345105845L;

    private final BinaryBloomFilter bloomFilter;
    private final ValueWorker<T> valueWorker;
    private final long recentValues;

    /**
     * Construct categorizer.
     *
     * @param bloomFilter bloom filter to determine if a value is referenced.
     * @param valueWorker serde for values and their asset keys.
     * @param maxSlopMicros minimum age of a value to consider it as being unreferenced.
     */
    public AssetCategorizer(BinaryBloomFilter bloomFilter, ValueWorker<T> valueWorker, long maxSlopMicros) {
      this.bloomFilter = bloomFilter;
      this.valueWorker = valueWorker;
      this.recentValues = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis()) - maxSlopMicros;
    }

    @Override
    public Iterator<CategorizedAssetKey> call(ValueFrame r) throws Exception {
      boolean referenced = r.getDt() > recentValues || bloomFilter.mightContain(r.getId());
      final Serializer<AssetKey> serializer = valueWorker.getAssetKeySerializer();
      T contents = valueWorker.fromBytes(ByteString.copyFrom(r.getBytes()));
      return valueWorker.getAssetKeys(contents).map(ak -> new CategorizedAssetKey(referenced, serializer.toBytes(ak))).iterator();
    }

  }

}
