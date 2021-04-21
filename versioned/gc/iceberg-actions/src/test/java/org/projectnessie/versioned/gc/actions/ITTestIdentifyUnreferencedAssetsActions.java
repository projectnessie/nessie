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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;

import java.io.File;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.GcTableCleanAction;
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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.versioned.gc.AssetKeySerializer;
import org.projectnessie.versioned.tiered.gc.DynamoSupplier;
import org.projectnessie.versioned.tiered.gc.GcOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

class ITTestIdentifyUnreferencedAssetsActions {
  private static final Logger LOGGER = LoggerFactory.getLogger(ITTestIdentifyUnreferencedAssetsActions.class);

  private static final String BRANCH = ITTestIdentifyUnreferencedAssetsActions.class.getName();
  private static final String DELETE_BRANCH = "toBeDeleted";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("test", "table");
  private static final TableIdentifier TABLE_IDENTIFIER2 = TableIdentifier.of("test", "table2");
  private static final Schema SCHEMA = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
                                                                      required(2, "foe2", Types.StringType.get())).fields());

  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19120);
  private static final String NESSIE_ENDPOINT = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @TempDir
  static File LOCAL_DIR;
  private static SparkSession spark;
  private static SparkSession sparkMain;
  private static SparkSession sparkDeleteBranch;


  protected NessieCatalog catalog;
  protected NessieClient client;
  protected TreeApi tree;
  protected ContentsApi contents;
  private NessieCatalog catalogDeleteBranch;
  private NessieCatalog catalogMainBranch;
  private AssetKeySerializer assetKeySerializer;

  @BeforeAll
  static void create() throws Exception {
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.catalog.spark_catalog.url", NESSIE_ENDPOINT)
        .set("spark.sql.catalog.spark_catalog.ref", BRANCH)
        .set("spark.sql.catalog.spark_catalog.warehouse", LOCAL_DIR.toURI().toString())
        .set("spark.sql.catalog.spark_catalog.catalog-impl", NessieCatalog.class.getName())
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .set("spark.sql.catalog.nessie.url", NESSIE_ENDPOINT)
        .set("spark.sql.catalog.nessie.ref", "main")
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
    sparkDeleteBranch.conf().set("spark.sql.catalog.spark_catalog.ref", DELETE_BRANCH);

    sparkMain = spark.newSession();
    sparkMain.conf().set("spark.sql.catalog.spark_catalog.ref", "main");
    sparkMain.conf().set("spark.sql.catalog.default_iceberg.ref", "main");
  }

  @BeforeEach
  void beforeEach() throws NessieConflictException, NessieNotFoundException {
    new DynamoSupplier().get();
    this.client = NessieClient.builder().withUri(NESSIE_ENDPOINT).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    resetData(tree);
    tree.createReference(Branch.of(BRANCH, null));
    tree.createReference(Branch.of(DELETE_BRANCH, null));

    Map<String, String> props = new HashMap<>();
    props.put("ref", BRANCH);
    props.put("url", NESSIE_ENDPOINT);
    props.put("warehouse", LOCAL_DIR.toURI().toString());
    Configuration hadoopConfig = spark.sessionState().newHadoopConf();
    catalog = (NessieCatalog) CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);
    assetKeySerializer = new AssetKeySerializer(new SerializableConfiguration(hadoopConfig));

    //second catalog for deleted branch
    props.put("ref", DELETE_BRANCH);
    hadoopConfig = sparkDeleteBranch.sessionState().newHadoopConf();
    catalogDeleteBranch = (NessieCatalog) CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);

    //third catalog for main branch
    props.put("ref", "main");
    hadoopConfig = sparkMain.sessionState().newHadoopConf();
    catalogMainBranch = (NessieCatalog) CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);
  }


  static void resetData(TreeApi tree) throws NessieConflictException, NessieNotFoundException {
    for (Reference r : tree.getAllReferences()) {
      if (r instanceof Branch) {
        tree.deleteBranch(r.getName(), r.getHash());
      } else {
        tree.deleteTag(r.getName(), r.getHash());
      }
    }
    tree.createReference(Branch.of("main", null));
  }

  @AfterEach
  void after() throws NessieNotFoundException, NessieConflictException {
    resetData(tree);
    DynamoSupplier.deleteAllTables();
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

    dataDF.write().format("iceberg").mode("append").save(tableIdentifier.toString());

  }

  @Test
  void run() throws Exception {

    // create an iceberg table on disk and stick some data in
    createTable(TABLE_IDENTIFIER, catalog);
    addFile(spark, TABLE_IDENTIFIER);

    // create a new table on a different branch, commit then delete the branch.
    createTable(TABLE_IDENTIFIER2, catalogDeleteBranch);
    addFile(sparkDeleteBranch, TABLE_IDENTIFIER2);
    client.getTreeApi().deleteBranch(DELETE_BRANCH, client.getTreeApi().getReferenceByName(DELETE_BRANCH).getHash());

    // now confirm that the unreferenced assets are marked for deletion. These are found based
    // on the no-longer referenced commit as well as the old commits.
    GcActions actions = new GcActions.Builder(sparkMain).setActionsConfig(actionsConfig()).setGcConfig(gcOptions(Clock.systemUTC()))
        .setTable(GcActions.DEFAULT_TABLE_IDENTIFIER).build();
    Dataset<Row> unreferencedAssets = actions.identifyUnreferencedAssets();
    actions.updateUnreferencedAssetTable(unreferencedAssets);

    //Test asset count
    //collect into a multimap for assertions
    Multimap<String, String> unreferencedItems = unreferencedAssets.collectAsList()
      .stream()
      .collect(Multimaps.toMultimap(x -> x.getString(4),
        x -> x.getString(5), HashMultimap::create));
    Map<String, Integer> count = unreferencedItems.keySet().stream()
        .collect(Collectors.toMap(Function.identity(), x -> unreferencedItems.get(x).size()));
    Set<String> paths = new HashSet<>(unreferencedItems.values());

    //1 table should be deleted from deleted branch
    //2 metadata: 1 from add and one from create
    //1 manifest lists: 1 from add
    //1 manifest: 1 manifest file from add
    //2 data files: 2 data files from table
    ImmutableMap<String, Integer> expected = ImmutableMap.of("TABLE", 1,
      "ICEBERG_MANIFEST", 1,
      "ICEBERG_MANIFEST_LIST", 1,
      "ICEBERG_METADATA", 2,
      "DATA_FILE", 2);
    assertThat(count.entrySet(), (Matcher)hasItems(expected.entrySet().toArray()));


    //delete second second branch and re-run asset identification
    client.getTreeApi().deleteBranch(BRANCH, client.getTreeApi().getReferenceByName(BRANCH).getHash());
    Dataset<Row> unreferencedAssets2 = actions.identifyUnreferencedAssets();
    actions.updateUnreferencedAssetTable(unreferencedAssets2);

    //Test asset count again to ensure we got both tables
    //collect into a multimap for assertions
    Multimap<String, String> unreferencedItems2 = unreferencedAssets2.collectAsList()
      .stream()
      .collect(Multimaps.toMultimap(x -> x.getString(4),
        x -> x.getString(5), HashMultimap::create));
    Map<String, Integer> count2 = unreferencedItems2.keySet().stream()
      .collect(Collectors.toMap(Function.identity(), x -> unreferencedItems2.get(x).size()));
    paths.addAll(unreferencedItems2.values());

    //4 table should be deleted from deleted branch  x2 tables
    //4 metadata: 1 from add and one from create x2 tables
    //2 manifest lists: 1 from add x2 tables
    //2 manifest: 1 manifest file from add x2 tables
    //4 data files: 2 data files from table x2 tables
    ImmutableMap<String, Integer> expected2 = ImmutableMap.of("TABLE", 2,
      "ICEBERG_MANIFEST", 2,
      "ICEBERG_MANIFEST_LIST", 2,
      "ICEBERG_METADATA", 4,
      "DATA_FILE", 4);
    assertThat(count2.entrySet(), (Matcher)hasItems(expected2.entrySet().toArray()));

    // now collect and remove both tables.
    Table table = catalogMainBranch.loadTable(GcActions.DEFAULT_TABLE_IDENTIFIER);
    GcTableCleanAction.GcTableCleanResult result =
        new GcTableCleanAction(table, sparkMain).dropGcTable(true).deleteCountThreshold(1).deleteOnPurge(false).execute();
    Assertions.assertEquals(result.getDeletedAssetCount(), 14);
    Assertions.assertEquals(result.getFailedDeletes(), 0);
    Assertions.assertEquals(result.getDeletedAssetCount(), paths.size());
    paths.forEach(p -> Assertions.assertFalse(new File(p).exists()));
  }

  private static GcActionsConfig actionsConfig() {
    return GcActionsConfig.builder().dynamoRegion("us-west-2").dynamoEndpoint("http://localhost:8000")
      .storeType(GcActionsConfig.StoreType.DYNAMO).build();
  }

  private static GcOptions gcOptions(Clock clock) {
    return GcOptions.builder()
      .bloomFilterCapacity(10_000_000)
      .timeSlopMicros(1)
      .maxAgeMicros(clock.millis() * 1000)
      .build();
  }
}
