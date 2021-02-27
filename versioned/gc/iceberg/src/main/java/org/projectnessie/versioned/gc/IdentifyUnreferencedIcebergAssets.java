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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.versioned.Serializer;

import com.google.protobuf.ByteString;

public class IdentifyUnreferencedIcebergAssets extends IdentifyUnreferencedAssets<Contents, IcebergAssetKey> {

  public IdentifyUnreferencedIcebergAssets(Serializer<Contents> valueSerializer, Serializer<AssetKey> assetKeySerializer,
      SparkSession spark) {
    super(valueSerializer, assetKeySerializer, init(spark), spark);
  }

  private static AssetKeyConverter<Contents, IcebergAssetKey> init(SparkSession spark) {
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    return new IcebergAssetKeyConverter(new SerializableConfiguration(hadoopConfig));
  }

  @Override
  public Dataset<UnreferencedItem> identify(Dataset<CategorizedValue> categorizedValues) {
    // todo this should change when the entity type changes.
    return super.identify(categorizedValues.filter(new ValueTypeFilter(super.valueSerializer)));
  }

  private static class ValueTypeFilter implements FilterFunction<CategorizedValue>, Serializable {

    private final Serializer<Contents> valueSerializer;

    private ValueTypeFilter(Serializer<Contents> valueSerializer) {
      this.valueSerializer = valueSerializer;
    }

    @Override
    public boolean call(CategorizedValue value) throws Exception {
      return valueSerializer.fromBytes(ByteString.copyFrom(value.getData())).getClass().equals(ImmutableIcebergTable.class);
    }
  }
}
