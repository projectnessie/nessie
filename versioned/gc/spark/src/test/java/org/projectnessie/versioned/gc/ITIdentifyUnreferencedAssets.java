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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;

class ITIdentifyUnreferencedAssets {

  private static final Multimap<String, GcTestUtils.DummyAsset> ASSETS = ImmutableMultimap.<String, GcTestUtils.DummyAsset>builder()
      .put("a", get(1))
      .put("a", get(2))
      .put("b", get(1))
      .put("c", get(1))
      .put("c", get(3))
      .build();

  private static GcTestUtils.DummyAsset get(int i) {
    return new GcTestUtils.DummyAsset(i);
  }

  @Test
  void test() {
    SparkSession spark = SparkSession
        .builder()
        .appName("test-nessie-gc-collection")
        .master("local[2]")
        .getOrCreate();


    IdentifyUnreferencedAssets<DummyValue, GcTestUtils.DummyAsset> ident = new IdentifyUnreferencedAssets<>(new DummyValueSerializer(),
        new GcTestUtils.DummyAssetKeySerializer(), new DummyAssetConverter(), spark);
    Dataset<IdentifyUnreferencedAssets.UnreferencedItem> items = ident.identify(generate(spark));
    Set<String> unreferencedItems = items.collectAsList().stream().map(IdentifyUnreferencedAssets.UnreferencedItem::getName)
        .collect(Collectors.toSet());
    assertThat(unreferencedItems, containsInAnyOrder("2"));
  }

  private Dataset<CategorizedValue> generate(SparkSession spark) {
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] {"a", "false"});
    stringAsList.add(new String[] {"b", "true"});
    stringAsList.add(new String[] {"c", "true"});

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(RowFactory::create);

    // Create schema
    StructType schema = DataTypes
        .createStructType(new StructField[] {
          DataTypes.createStructField("id", DataTypes.StringType, false),
          DataTypes.createStructField("referenced", DataTypes.StringType, false)
        });
    DummyValueSerializer ser = new DummyValueSerializer();

    return spark.sqlContext().createDataFrame(rowRDD, schema)
         .map((MapFunction<Row, CategorizedValue>) x -> {
           boolean referenced = Boolean.parseBoolean(x.getString(1));
           DummyValue value = new DummyValue(x.getString(0));
           ByteString data = ser.toBytes(value);
           return new CategorizedValue(referenced, data, 0L);
         }, Encoders.bean(CategorizedValue.class));
  }

  private static class DummyValueSerializer extends GcTestUtils.JsonSerializer<DummyValue> implements Serializable {

    public DummyValueSerializer() {
      super(DummyValue.class);
    }

    @Override
    public Byte getPayload(DummyValue value) {
      return null;
    }
  }

  private static class DummyAssetConverter implements AssetKeyConverter<DummyValue, GcTestUtils.DummyAsset>, Serializable {

    @Override
    public Stream<GcTestUtils.DummyAsset> apply(DummyValue value) {
      return ASSETS.get(value.getId()).stream();
    }
  }

  private static class DummyValue {

    private final String id;

    @JsonCreator
    public DummyValue(@JsonProperty("id")String id) {
      super();
      this.id = id;

    }

    public String getId() {
      return id;
    }

  }

}
