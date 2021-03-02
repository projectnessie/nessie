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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.Actions;
import org.apache.iceberg.actions.ExpireSnapshotsActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.Reference;
import org.projectnessie.server.providers.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.Serializer;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.dynamodb.LocalDynamoDB;
import org.projectnessie.versioned.tiered.gc.DynamoSupplier;
import org.projectnessie.versioned.tiered.gc.GcOptions;
import org.projectnessie.versioned.tiered.gc.IdentifyUnreferencedValues;
import org.projectnessie.versioned.tiered.gc.ImmutableGcOptions;
import org.projectnessie.versioned.tiered.gc.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.protobuf.ByteString;

@ExtendWith(LocalDynamoDB.class)
class ITTestIdentifyUnreferencedAssetsIceberg {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITTestIdentifyUnreferencedAssetsIceberg.class);

  private static final Consumer<String> NOOP = x -> {};
  private static final String BRANCH = ITTestIdentifyUnreferencedAssetsIceberg.class.getName();
  private static final String DELETE_BRANCH = "toBeDeleted";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("test", "table");
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of("test", "table2");
  private static final Schema SCHEMA = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());

  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);
  private static final String NESSIE_ENDPOINT = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @TempDir
  static File LOCAL_DIR;
  private static SparkSession spark;
  private static SparkSession sparkDeleteBranch;

  private StoreWorker<Contents, CommitMeta> helper;


  protected NessieCatalog catalog;
  protected NessieClient client;
  protected TreeApi tree;
  protected ContentsApi contents;
  private NessieCatalog catalogDeleteBranch;

  @BeforeAll
  static void create() throws Exception {
    new DynamoSupplier().get(); // make sure tables are created
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.nessie.url", NESSIE_ENDPOINT)
        .set("spark.sql.catalog.nessie.ref", BRANCH)
        .set("spark.sql.catalog.nessie.warehouse", LOCAL_DIR.toURI().toString())
        .set("spark.sql.catalog.nessie.catalog-impl", NessieCatalog.class.getName())
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.default_iceberg.url", NESSIE_ENDPOINT)
        .set("spark.sql.catalog.default_iceberg.ref", BRANCH)
        .set("spark.sql.catalog.default_iceberg.warehouse", LOCAL_DIR.toURI().toString())
        .set("spark.sql.catalog.default_iceberg.catalog-impl", NessieCatalog.class.getName())
        .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set(SQLConf.PARTITION_OVERWRITE_MODE().key(), "dynamic");
    spark = SparkSession
      .builder()
      .appName("test-nessie-gc-iceberg")
      .config(conf)
      .master("local[2]")
      .getOrCreate();

    //create new spark session for 2nd branch committer
    sparkDeleteBranch = spark.newSession();
    sparkDeleteBranch.conf().set("spark.sql.catalog.nessie.ref", DELETE_BRANCH);
  }

  private void resetData() throws NessieConflictException, NessieNotFoundException {
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch) {
        tree.deleteBranch(r.getName(), r.getHash());
      } else {
        tree.deleteTag(r.getName(), r.getHash());
      }
    }
    tree.createReference(Branch.of("main", null));
    tree.createReference(Branch.of(BRANCH, null));
    client.getTreeApi().createReference(Branch.of(DELETE_BRANCH, null));
  }

  @BeforeEach
  void beforeEach() throws NessieConflictException, NessieNotFoundException {
    this.client = NessieClient.builder().withUri(NESSIE_ENDPOINT).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    resetData();

    Map<String, String> props = new HashMap<>();
    props.put("ref", BRANCH);
    props.put("url", NESSIE_ENDPOINT);
    props.put("warehouse", LOCAL_DIR.toURI().toString());
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    catalog = (NessieCatalog) CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);

    //second catalog for deleted branch
    props.put("ref", DELETE_BRANCH);
    hadoopConfig = sparkDeleteBranch.sessionState().newHadoopConf();
    catalogDeleteBranch = (NessieCatalog) CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);

    helper = new TableCommitMetaStoreWorker();
  }


  @AfterEach
  void after() {
    helper = null;
  }


  private Table createTable(TableIdentifier tableIdentifier, NessieCatalog catalog) {
    try {
      return catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.builderFor(SCHEMA).identity("foe1").build());
    } catch (Throwable t) {
      LOGGER.error("unable to do create {}", tableIdentifier, t);
      throw t;
    }
  }

  private void addFile(SparkSession spark, TableIdentifier tableIdentifier) {
    List<String[]> stringAsList = new ArrayList<>();
    stringAsList.add(new String[] {"bar1.1", "bar2.1"});
    stringAsList.add(new String[] {"bar1.2", "bar2.2"});

    JavaSparkContext sparkContext = new JavaSparkContext(spark.sparkContext());

    JavaRDD<Row> rowRDD = sparkContext.parallelize(stringAsList).map(RowFactory::create);

    // Create schema
    StructType schema = DataTypes
        .createStructType(new StructField[] {
          DataTypes.createStructField("foe1", DataTypes.StringType, false),
          DataTypes.createStructField("foe2", DataTypes.StringType, false)
        });

    Dataset<Row> dataDF = spark.sqlContext().createDataFrame(rowRDD, schema);

    dataDF.write().format("iceberg").mode("append").save("nessie." + tableIdentifier);

  }

  @Test
  void run() throws Exception {

    // create an iceberg table on disk and stick some data in
    Table table = createTable(TABLE_IDENTIFIER, catalog);
    addFile(spark, TABLE_IDENTIFIER);

    // delete some data
    spark.sql(String.format("DELETE FROM nessie.%s where foe1 = 'bar1.1'", TABLE_IDENTIFIER)).collectAsList();

    // create a new table on a different branch, commit then delete the branch.
    createTable(TABLE_IDENTIFIER2, catalogDeleteBranch);
    addFile(sparkDeleteBranch, TABLE_IDENTIFIER2);
    client.getTreeApi().deleteBranch(DELETE_BRANCH, client.getTreeApi().getReferenceByName(DELETE_BRANCH).getHash());

    // hack: sleep for 10 seconds so GC will see above data as old
    Thread.sleep(10 * 1000);

    // add some more data to first table
    addFile(spark, TABLE_IDENTIFIER);

    long commitTime = System.currentTimeMillis();
    ExpireSnapshotsActionResult actualExpireResults =
        Actions.forTable(spark, table).expireSnapshots().expireOlderThan(commitTime).deleteWith(NOOP).execute();

    // double check expire does what we expect
    assertEquals(1L, actualExpireResults.dataFilesDeleted());
    assertEquals(1L, actualExpireResults.manifestFilesDeleted());
    assertEquals(2L, actualExpireResults.manifestListsDeleted());

    // now confirm that the unreferenced assets are marked for deletion. These are found based
    // on the no-longer referenced commit as well as the old commits.
    GcOptions options = ImmutableGcOptions.builder()
        .bloomFilterCapacity(10_000_000)
        .timeSlopMicros(1)
        .maxAgeMicros((System.currentTimeMillis() - commitTime) * 1000)
        .build();
    IdentifyUnreferencedValues<Contents> app = new IdentifyUnreferencedValues<>(helper, new DynamoSupplier(), spark, options,
        new SystemClock());
    Dataset<CategorizedValue> values = app.identify();

    SerializableConfiguration hadoopConfig = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    AssetKeySerializer assetKeySerializer = new AssetKeySerializer(hadoopConfig);
    Serializer<Contents> valueSerializer = helper.getValueSerializer();
    IcebergAssetKeyConverter converter = new IcebergAssetKeyConverter(hadoopConfig);
    IdentifyUnreferencedAssets<Contents, IcebergAssetKey> assets =
        new IdentifyUnreferencedAssets<>(valueSerializer, assetKeySerializer, converter, new ValueTypeFilter(valueSerializer), spark);
    Dataset<IdentifyUnreferencedAssets.UnreferencedItem> items = assets.identify(values);

    //collect into a multimap for assertions
    Multimap<String, AssetKey> unreferencedItems = items.collectAsList()
        .stream()
        .collect(Multimaps.toMultimap(IdentifyUnreferencedAssets.UnreferencedItem::getName,
            x -> assetKeySerializer.fromBytes(ByteString.copyFrom(x.getAsset())), HashMultimap::create));
    Map<String, Long> count = unreferencedItems.keySet().stream()
        .map(x -> x.split("\\.")[0]).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    //1 table should be deleted from deleted branch
    //6 metadata: 2 from add, 1 from delete, 2 from creates and 1 from add on other branch
    //3 manifest lists: 1 from add, 1 from delete, 1 from add on branch
    //2 manifests: 1 manifest file from deleted table and 1 from first add
    //2 data files: 2 data files from deleted table
    ImmutableMap<String, Long> expected = ImmutableMap.of("TABLE", 1L,
        "ICEBERG_MANIFEST", 2L,
        "ICEBERG_MANIFEST_LIST", 3L,
        "ICEBERG_METADATA", 6L,
        "DATA_FILE", 2L);
    assertThat(count.entrySet(), (Matcher)hasItems(expected.entrySet().toArray()));

    //delete all the unused data
    unreferencedItems.values().forEach(AssetKey::delete);

    // assert that we can still read the data and that the table count is correct
    final long[] finalCount = {0};
    assertDoesNotThrow(() -> {
      finalCount[0] = (long) spark.sql("SELECT COUNT(*) from nessie." + TABLE_IDENTIFIER).collectAsList().get(0).get(0);
    });
    assertEquals(3, finalCount[0]);

    // assert correct set of files still exists
    // we dont check exact file names as we can't know uuid. So we count extensions and directories
    List<Path> paths = Files.walk(LOCAL_DIR.toPath()).filter(Files::isRegularFile).collect(Collectors.toList());
    Map<String, Long> existingPathCount = paths.stream()
        .map(x -> x.toString().replace(LOCAL_DIR.toString() + "/test/table/", ""))
        .map(x -> x.split("/",2)[0])
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    Map<String, Long> extensionCount = paths.stream()
        .map(Path::toString)
        .map(f -> f.substring(f.lastIndexOf(".") + 1))
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    //ensure no files from table2 are left
    assertFalse(paths.stream().anyMatch(x -> x.toString().contains("table2")));

    //all 4 data files still exist (2x adds) 4 crc files
    //1 metadata, 1 manifest list and 2 manifests (1 for each partition) + 4 crc files
    ImmutableMap<String, Long> expectedPathCount = ImmutableMap.of("metadata", 8L, "data", 8L);
    assertThat(existingPathCount.entrySet(), (Matcher)hasItems(expectedPathCount.entrySet().toArray()));

    // 4 parquet for 4 data as above
    // 8 crc as above
    // 3 avro for 2 manifests and 1 manifest list
    // 1 json metadata
    ImmutableMap<String, Long> expectedExtensionCount = ImmutableMap.of("crc", 8L, "json", 1L, "avro", 3L, "parquet", 4L);
    assertThat(extensionCount.entrySet(), (Matcher)hasItems(expectedExtensionCount.entrySet().toArray()));

    // 4 metadata + 4 data + 8 crc
    assertEquals(16, paths.size());
  }

}
