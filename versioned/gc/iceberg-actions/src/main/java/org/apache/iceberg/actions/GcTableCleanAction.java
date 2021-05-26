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
package org.apache.iceberg.actions;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.versioned.gc.AssetKey;
import org.projectnessie.versioned.gc.AssetKeySerializer;
import org.projectnessie.versioned.gc.actions.GcActionUtils;
import scala.Tuple2;

/**
 * Iceberg Action to clean the GC table
 *
 * <p>note: not using a builder to set params to stay in line w/ how Iceberg constructs actions.
 */
public class GcTableCleanAction extends BaseSparkAction<GcTableCleanAction.GcTableCleanResult> {
  private final Table table;
  private final SparkSession spark;
  private final AssetKeySerializer assetKeySerializer;
  private long seenCount = 10;
  private boolean deleteOnPurge = true;
  private boolean dropGcTable = true;

  /** Construct an action to clean the GC table. */
  public GcTableCleanAction(Table table, SparkSession spark) {
    this.table = table;
    this.spark = spark;
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    SerializableConfiguration configuration = new SerializableConfiguration(hadoopConfig);
    this.assetKeySerializer = new AssetKeySerializer(configuration);
  }

  public GcTableCleanAction deleteOnPurge(boolean deleteOnPurge) {
    this.deleteOnPurge = deleteOnPurge;
    return this;
  }

  public GcTableCleanAction dropGcTable(boolean dropGcTable) {
    this.dropGcTable = dropGcTable;
    return this;
  }

  public GcTableCleanAction deleteCountThreshold(long seenCount) {
    this.seenCount = seenCount;
    return this;
  }

  @Override
  protected Table table() {
    return table;
  }

  @Override
  public GcTableCleanResult execute() {
    Dataset<Row> purgeResult = purgeUnreferencedAssetTable();
    if (deleteOnPurge) {
      cleanUnreferencedAssetTable(purgeResult, dropGcTable);
    }
    Row count =
        purgeResult
            .withColumn("deleted", purgeResult.col("_2").cast("int"))
            .agg(ImmutableMap.of("deleted", "sum", "_2", "count"))
            .first();

    return new GcTableCleanResult(count.getLong(1), count.getLong(1) - count.getLong(0));
  }

  private String tableName() {
    return String.format("%s", table.name());
  }

  private Dataset<Row> purgeUnreferencedAssetTable() {
    long currentRunId = GcActionUtils.getMaxRunId(spark, table.name());
    Dataset<Row> deletable =
        spark.sql(
            String.format(
                "SELECT count(*) as counted, name, last(timestamp) as timestamp, last(asset) as asset, max(runid) as runid FROM %s "
                    + "GROUP BY name HAVING counted >= %d AND runid = %d",
                tableName(), seenCount, currentRunId));

    Dataset<Tuple2<String, Boolean>> deletes =
        deletable.map(
            new DeleteFunction(assetKeySerializer),
            Encoders.tuple(Encoders.STRING(), Encoders.BOOLEAN()));
    return deletable
        .joinWith(deletes, deletable.col("name").equalTo(deletes.col(deletes.columns()[0])))
        .select("_1.counted", "_1.name", "_1.timestamp", "_1.asset", "_1.runid", "_2._2");
  }

  private void cleanUnreferencedAssetTable(Dataset<Row> deleted, boolean purge) {
    if (purge) {
      spark.sql(String.format("DROP TABLE %s", tableName()));
    } else {
      // todo
      throw new UnsupportedOperationException("Not Yet Implemented");
    }
  }

  public static class GcTableCleanResult {
    private final long deletedAssetCount;
    private final long failedDeletes;

    public GcTableCleanResult(long deletedAssetCount, long failedDeletes) {
      this.deletedAssetCount = deletedAssetCount;
      this.failedDeletes = failedDeletes;
    }

    public long getDeletedAssetCount() {
      return deletedAssetCount;
    }

    public long getFailedDeletes() {
      return failedDeletes;
    }
  }

  private static class DeleteFunction implements MapFunction<Row, Tuple2<String, Boolean>> {
    private final AssetKeySerializer assetKeySerializer;

    private DeleteFunction(AssetKeySerializer assetKeySerializer) {
      this.assetKeySerializer = assetKeySerializer;
    }

    @Override
    public Tuple2<String, Boolean> call(Row value) throws Exception {
      AssetKey assetKey = assetKeySerializer.fromBytes(ByteString.copyFrom((byte[]) value.get(3)));
      return Tuple2.apply(value.getString(1), assetKey.delete().toCompletableFuture().get());
    }
  }
}
