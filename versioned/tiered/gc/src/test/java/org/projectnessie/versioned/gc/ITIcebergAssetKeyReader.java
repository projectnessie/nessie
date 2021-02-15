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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.spark.util.SerializableConfiguration;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.AssetKey;
import org.projectnessie.versioned.dynamodb.LocalDynamoDB;
import org.projectnessie.versioned.gc.assets.IcebergAssetKeyReader;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

@ExtendWith(LocalDynamoDB.class)
public class ITIcebergAssetKeyReader {
  private static final int NESSIE_PORT = Integer.getInteger("quarkus.http.test-port", 19121);
  private static final String NESSIE_ENDPOINT = String.format("http://localhost:%d/api/v1", NESSIE_PORT);

  @TempDir
  static File ALLEY_LOCAL_DIR;
  private static final Schema SCHEMA = new Schema(Types.StructType.of(required(1, "foe1", Types.StringType.get()),
      required(2, "foe2", Types.StringType.get())).fields());
  private NessieClient client;
  private TreeApi tree;
  private Catalog catalog;
  private Configuration hadoopConfig;

  @BeforeAll
  static void getDynamo() {
    new DynamoSupplier().get();
  }

  @BeforeEach
  void init() throws NessieNotFoundException, NessieConflictException {
    this.client = NessieClient.none(NESSIE_ENDPOINT);
    tree = client.getTreeApi();
    tree.createReference(Branch.of("main", null));

    Map<String, String> props = new HashMap<>();
    props.put("ref", "main");
    props.put("url", NESSIE_ENDPOINT);
    props.put("warehouse", ALLEY_LOCAL_DIR.toURI().toString());
    hadoopConfig = new Configuration();
    catalog = CatalogUtil.loadCatalog(NessieCatalog.class.getName(), "nessie", props, hadoopConfig);
  }

  @Test
  void testAssetKeyReader() {


    Table table = catalog.createTable(TableIdentifier.of("test", "table"), SCHEMA);

    table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned()).withPath("/x/y/z")
      .withFormat(FileFormat.PARQUET).withFileSizeInBytes(12L).withRecordCount(12).build()).commit();

    IcebergAssetKeyReader<Object> akr =
        new IcebergAssetKeyReader<>(IcebergTable.of(((BaseTable) table).operations().current().metadataFileLocation()),
            new SerializableConfiguration(hadoopConfig));

    List<AssetKey> fileList = akr.get().collect(Collectors.toList());

    Multimap<String, AssetKey> unreferencedItems = fileList
        .stream()
        .collect(Multimaps.toMultimap(x -> String.join(".", x.toReportableName()), x -> x, ArrayListMultimap::create));
    Map<String, Long> count = unreferencedItems.keySet().stream()
        .map(x -> x.split("\\.")[0]).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    // 1 of each as a single commit was checked
    ImmutableMap<String, Long> expected = ImmutableMap.of("TABLE", 1L,
        "ICEBERG_MANIFEST", 1L,
        "ICEBERG_MANIFEST_LIST", 1L,
        "ICEBERG_METADATA", 1L,
        "DATA_FILE", 1L);
    assertThat(count.entrySet(), (Matcher) hasItems(expected.entrySet().toArray()));

    table.newAppend().appendFile(DataFiles.builder(PartitionSpec.unpartitioned()).withPath("/x/y/zz")
      .withFormat(FileFormat.PARQUET).withFileSizeInBytes(12L).withRecordCount(12).build()).commit();

    IcebergAssetKeyReader<Object> akr2 =
        new IcebergAssetKeyReader<>(IcebergTable.of(((BaseTable) table).operations().current().metadataFileLocation()),
            new SerializableConfiguration(hadoopConfig));

    List<AssetKey> fileList2 = akr2.get().collect(Collectors.toList());

    Multimap<String, AssetKey> unreferencedItems2 = fileList2
        .stream()
        .collect(Multimaps.toMultimap(x -> String.join(".", x.toReportableName()), x -> x, ArrayListMultimap::create));
    Map<String, Long> count2 = unreferencedItems2.keySet().stream()
        .map(x -> x.split("\\.")[0]).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

    ImmutableMap<String, Long> expected2 = ImmutableMap.of("TABLE", 1L, //still 1 table
        "ICEBERG_MANIFEST", 2L, // 1 manifest from first commit, 1 from second
        "ICEBERG_MANIFEST_LIST", 1L, // always one manifest list per commit
        "ICEBERG_METADATA", 1L, // always 1 metadata file per commit
        "DATA_FILE", 2L); // 2 data files, 1 for each append
    assertThat(count2.entrySet(), (Matcher) hasItems(expected2.entrySet().toArray()));
  }

  @AfterEach
  void stop() throws NessieNotFoundException, NessieConflictException {
    tree.deleteBranch("main", tree.getReferenceByName("main").getHash());
    client.close();
  }

}
