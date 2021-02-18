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

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.versioned.Serializer;

public class IdentifyUnreferencedIcebergAssets extends IdentifyUnreferencedAssets<Contents> {

  public IdentifyUnreferencedIcebergAssets(Serializer<Contents> valueSerializer, Serializer<AssetKey> assetKeySerializer,
      SparkSession spark) {
    super(valueSerializer, assetKeySerializer, init(spark), spark);
  }

  private static AssetKeyConverter<Contents> init(SparkSession spark) {
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    return new IcebergAssetKeyConverter(new SerializableConfiguration(hadoopConfig));
  }

  @Override
  public Dataset<UnreferencedItem> identify(Dataset<CategorizedValue> categorizedValues) {
    // todo this should change when the entity type changes.
    Dataset<CategorizedValue> filtered = categorizedValues.filter((FilterFunction<CategorizedValue>) value ->
        value.getValueType().equals(ImmutableIcebergTable.class.getSimpleName()));
    return super.identify(filtered);
  }
}
