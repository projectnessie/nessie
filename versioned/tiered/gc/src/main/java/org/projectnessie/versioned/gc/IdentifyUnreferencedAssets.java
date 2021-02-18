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
package org.projectnessie.versioned.gc;

import java.io.Serializable;
import java.util.Iterator;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.gc.core.AssetKey;
import org.projectnessie.versioned.gc.core.AssetKeyConverter;
import org.projectnessie.versioned.gc.core.BinaryBloomFilter;
import org.projectnessie.versioned.gc.core.CategorizedValue;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

import scala.Function1;

/**
 * Operation which identifies unreferenced assets.
 */
public class IdentifyUnreferencedAssets<T> {

  private final StoreWorker<T, ?> storeWorker;
  private final Serializer<AssetKey> assetKeySerializer;
  private final AssetKeyConverter<T> assetKeyConverter;
  private final Dataset<CategorizedValue> categorizedValues;
  private final SparkSession spark;

  /**
   * Drive a job that generates a dataset of unreferenced assets.
   */
  public IdentifyUnreferencedAssets(
      StoreWorker<T, ?> storeWorker,
      Serializer<AssetKey> assetKeySerializer,
      AssetKeyConverter<T> assetKeyConverter,
      Dataset<CategorizedValue> categorizedValues,
      SparkSession spark) {
    super();
    this.storeWorker = storeWorker;
    this.assetKeySerializer = assetKeySerializer;
    this.assetKeyConverter = assetKeyConverter;
    this.categorizedValues = categorizedValues;
    this.spark = spark;
  }

  public Dataset<UnreferencedItem> identify() {
    return go(storeWorker, categorizedValues, assetKeySerializer, assetKeyConverter, spark);
  }

  private static <T> Dataset<UnreferencedItem> go(StoreWorker<T, ?> storeWorker, Dataset<CategorizedValue> categorizedValues,
      Serializer<AssetKey> assetKeySerializer, AssetKeyConverter<T> assetKeyConverter, SparkSession spark) {

    // If it is, generate a referenced asset. If not, generate a non-referenced asset.
    // this is a single output that has a categorization column
    AssetFlatMapper<T> mapper = new AssetFlatMapper<T>(storeWorker.getValueSerializer(), assetKeySerializer, assetKeyConverter);
    Dataset<CategorizedAssetKey> assets = categorizedValues.flatMap(mapper, Encoders.bean(CategorizedAssetKey.class));

    // generate a bloom filter of referenced items.
    final BinaryBloomFilter referencedAssets = BinaryBloomFilter.aggregate(assets.filter("referenced = true").select("data"), "data");

    // generate list of maybe referenced assets (note that a single asset may be referenced by both referenced and non-referenced values).
    // TODO: convert this to a group by asset, date and then figure out the latest date so we can include that
    // in the written file to avoid stale -> not stale -> stale values.
    Dataset<Row> unreferencedAssets = assets.filter("referenced = false").select("data").filter(new AssetFilter(referencedAssets));

    // map the generic spark Row back to a concrete type.
    return unreferencedAssets.map(
        new UnreferencedItemConverter(assetKeySerializer), Encoders.bean(UnreferencedItem.class));
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
   * Spark flat map function to convert a value into an iterator of AssetKeys keeping their reference state.
   */
  public static class AssetFlatMapper<T> implements FlatMapFunction<CategorizedValue, CategorizedAssetKey> {

    private static final long serialVersionUID = -4605489080345105845L;

    private final Serializer<T> valueWorker;
    private final Serializer<AssetKey> assetKeySerializer;
    private final AssetKeyConverter<T> assetKeyConverter;

    /**
     * Construct mapper.
     *  @param valueWorker serde for values and their asset keys.
     * @param assetKeySerializer locate and serialize AssetKeys
     * @param assetKeyConverter convert value of type T to its associated Asset Keys
     */
    public AssetFlatMapper(Serializer<T> valueWorker, Serializer<AssetKey> assetKeySerializer, AssetKeyConverter<T> assetKeyConverter) {
      this.valueWorker = valueWorker;
      this.assetKeySerializer = assetKeySerializer;
      this.assetKeyConverter = assetKeyConverter;
    }

    @Override
    public Iterator<CategorizedAssetKey> call(CategorizedValue r) throws Exception {
      T contents = valueWorker.fromBytes(ByteString.copyFrom(r.getData()));
      return assetKeyConverter.getAssetKeys(contents)
        .map(ak -> new CategorizedAssetKey(r.isReferenced(), assetKeySerializer.toBytes(ak))).iterator();
    }

  }

}
