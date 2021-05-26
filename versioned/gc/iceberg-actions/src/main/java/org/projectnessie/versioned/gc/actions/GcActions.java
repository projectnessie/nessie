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
package org.projectnessie.versioned.gc.actions;

import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.projectnessie.model.Contents;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.dynamodb.DynamoStore;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;
import org.projectnessie.versioned.gc.AssetKeySerializer;
import org.projectnessie.versioned.gc.CategorizedValue;
import org.projectnessie.versioned.gc.IcebergAssetKey;
import org.projectnessie.versioned.gc.IcebergAssetKeyConverter;
import org.projectnessie.versioned.gc.IdentifyUnreferencedAssets;
import org.projectnessie.versioned.gc.ValueTypeFilter;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.tiered.gc.GcOptions;
import org.projectnessie.versioned.tiered.gc.IdentifyUnreferencedValues;
import software.amazon.awssdk.regions.Region;

/** Identify unreferenced assets and update the gc table. */
public class GcActions {
  public static final TableIdentifier DEFAULT_TABLE_IDENTIFIER =
      TableIdentifier.parse("gc.identified_tables");
  private static final StructType SCHEMA =
      SparkSchemaUtil.convert(
          new Schema(
              Types.StructType.of(
                      required(1, "tableName", Types.StringType.get()),
                      required(2, "timestamp", Types.TimestampType.withZone()),
                      required(3, "asset", Types.BinaryType.get()),
                      required(4, "snapshotId", Types.StringType.get()),
                      required(5, "assetType", Types.StringType.get()),
                      required(6, "path", Types.StringType.get()),
                      required(7, "name", Types.StringType.get()),
                      required(8, "runid", Types.LongType.get()))
                  .fields()));

  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
  private final Clock clock = Clock.systemUTC();
  private final SparkSession spark;
  private final AssetKeySerializer assetKeySerializer;
  private final IcebergAssetKeyConverter assetKeyConverter;
  private final GcActionsConfig actionsConfig;
  private final GcOptions gcConfig;
  private final TableIdentifier table;

  private GcActions(
      SparkSession spark,
      GcActionsConfig actionsConfig,
      GcOptions gcConfig,
      TableIdentifier table) {
    this.spark = spark;
    this.actionsConfig = actionsConfig;
    this.gcConfig = gcConfig;
    this.table = table;
    SparkConf conf = new SparkConf();
    conf.setAll(spark.sessionState().conf().getAllConfs());
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    SerializableConfiguration configuration = new SerializableConfiguration(hadoopConfig);
    this.assetKeySerializer = new AssetKeySerializer(configuration);
    this.assetKeyConverter = new IcebergAssetKeyConverter(configuration);
    createTable(table);
  }

  private SparkSession spark() {
    return spark;
  }

  /** Build Spark dataset of unreferenced assets. This is in the schema of the gc table. */
  public Dataset<Row> identifyUnreferencedAssets() throws AnalysisException {
    IdentifyUnreferencedValues<Contents> values =
        new IdentifyUnreferencedValues<>(worker, store(actionsConfig), spark(), gcConfig, clock);
    Dataset<CategorizedValue> unreferencedValues = values.identify();
    IdentifyUnreferencedAssets<Contents, IcebergAssetKey> assets =
        new IdentifyUnreferencedAssets<>(
            worker.getValueSerializer(),
            assetKeySerializer,
            assetKeyConverter,
            new ValueTypeFilter(worker.getValueSerializer()),
            spark());
    Dataset<IdentifyUnreferencedAssets.UnreferencedItem> unreferencedAssets =
        assets.identify(unreferencedValues);
    long currentRunId = GcActionUtils.getMaxRunId(spark, table.toString()) + 1;
    return unreferencedAssets
        .map(new ConvertToTableFunction(assetKeySerializer), RowEncoder.apply(SCHEMA))
        .withColumn("runid", functions.lit(currentRunId));
  }

  /** Append dataset to the gc table. Dataset is already in the correct schema. */
  public void updateUnreferencedAssetTable(Dataset<Row> unreferencedAssets) {
    // sort because of https://issues.apache.org/jira/browse/SPARK-23889
    unreferencedAssets
        .repartition(unreferencedAssets.col("tableName"))
        .sortWithinPartitions()
        .write()
        .format("iceberg")
        .mode("append")
        .save(table.toString());
  }

  private static class ConvertToTableFunction
      implements MapFunction<IdentifyUnreferencedAssets.UnreferencedItem, Row> {
    private final AssetKeySerializer assetKeySerializer;

    private ConvertToTableFunction(AssetKeySerializer assetKeySerializer) {
      this.assetKeySerializer = assetKeySerializer;
    }

    @Override
    public Row call(IdentifyUnreferencedAssets.UnreferencedItem value) throws Exception {
      IcebergAssetKey assetKey =
          (IcebergAssetKey)
              assetKeySerializer.fromBytes(ByteString.copyFrom((byte[]) value.getAsset()));
      List<String> key = value.getKey();
      long microTimestamp = value.getTimestamp();
      long secondTimestamp = TimeUnit.MICROSECONDS.toSeconds(microTimestamp);
      long nanos = microTimestamp * 1_000 - secondTimestamp * 1_000_000_000;
      Timestamp timestamp = Timestamp.from(Instant.ofEpochSecond(secondTimestamp, nanos));

      return RowFactory.create(
          String.join(".", key),
          timestamp,
          value.getAsset(),
          assetKey.getSnapshotId(),
          assetKey.getType().toString(),
          assetKey.getPath(),
          String.join(".", assetKey.toReportableName()));
    }
  }

  static DynamoStore createStore(GcActionsConfig config) {
    return new DynamoStore(
        DynamoStoreConfig.builder()
            .endpoint(
                Optional.ofNullable(config.getDynamoEndpoint())
                    .map(
                        e -> {
                          try {
                            return new URI(e);
                          } catch (URISyntaxException ex) {
                            throw new RuntimeException(ex);
                          }
                        }))
            .region(Region.of(config.getDynamoRegion()))
            .build());
  }

  private Supplier<Store> store(GcActionsConfig config) {
    if (config.getStoreType() != GcActionsConfig.StoreType.DYNAMO) {
      throw new UnsupportedOperationException("Ony dynamo tiered store is supported");
    }
    Store store = createStore(config);
    store.start();
    return () -> store;
  }

  private void createTable(TableIdentifier tableIdentifier) {
    CatalogPlugin catalog = spark.sessionState().catalogManager().currentCatalog();
    Identifier ident = Identifier.of(tableIdentifier.namespace().levels(), tableIdentifier.name());
    Transform[] partitions =
        Spark3Util.toTransforms(
            PartitionSpec.builderFor(SparkSchemaUtil.convert(SCHEMA))
                .identity("tableName")
                .build());
    try {
      ((TableCatalog) catalog).createTable(ident, SCHEMA, partitions, ImmutableMap.of());
    } catch (TableAlreadyExistsException e) {
      // table already exists. Does it have the same catalog?
      try {
        if (!((TableCatalog) catalog).loadTable(ident).schema().equals(SCHEMA)) {
          throw new RuntimeException(
              String.format(
                  "Cannot create table %s. Table with different schema already exists", ident),
              e);
        }
      } catch (NoSuchTableException noSuchTableException) {
        // can't happen
      }
    } catch (NoSuchNamespaceException e) {
      // this can't happen when using a Nessie Catalog as namespaces are implicit. If this happens
      // you are likely not using a Nessie catalog
      throw new RuntimeException(
          String.format(
              "Cannot create table. Are you using a Nessie Catalog. Catalog is %s",
              catalog.getClass().getName()),
          e);
    }
  }

  public static class Builder {
    private final SparkSession spark;
    private GcActionsConfig actionsConfig;
    private GcOptions gcOptions;
    private TableIdentifier table;

    public Builder(SparkSession spark) {
      this.spark = spark;
    }

    public Builder setActionsConfig(GcActionsConfig actionsConfig) {
      this.actionsConfig = actionsConfig;
      return this;
    }

    public Builder setGcOptions(GcOptions gcOptions) {
      this.gcOptions = gcOptions;
      return this;
    }

    public Builder setTable(TableIdentifier table) {
      this.table = table;
      return this;
    }

    public GcActions build() {
      return new GcActions(spark, actionsConfig, gcOptions, table);
    }
  }
}
