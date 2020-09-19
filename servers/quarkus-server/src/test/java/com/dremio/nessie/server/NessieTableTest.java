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

package com.dremio.nessie.server;


import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.PutContents;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@QuarkusTest
class NessieTableTest extends BaseTestIceberg {

  private static final String BRANCH = "iceberg-table-test";

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final ContentsKey KEY = ContentsKey.of(DB_NAME, TABLE_NAME);
  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final Schema altered = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.LongType.get())).fields());

  private Path tableLocation;

  public NessieTableTest() {
    super(BRANCH);
  }

  @BeforeEach
  public void beforeEach() {
    super.beforeEach();
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());
  }

  @AfterEach
  public void afterEach() throws Exception {
    // drop the table data
    if (tableLocation != null) {
      tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
      catalog.refresh();
      catalog.dropTable(TABLE_IDENTIFIER, false);
    }

    super.afterEach();
  }

  private com.dremio.nessie.model.IcebergTable getTable(ContentsKey key) {
    return client.getContentsApi()
        .getContents(BRANCH, key)
        .unwrap(IcebergTable.class).get();
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testCreate() {
    // Table should be created in alley
    // Table should be renamed in alley
    String tableName = TABLE_IDENTIFIER.name();
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    IcebergTable table = getTable(KEY);
    // check parameters are in expected state
    Assertions.assertEquals(getTableLocation(tableName),
                            (ALLEY_LOCAL_DIR.toURI().toString() + "iceberg/warehouse/" + DB_NAME + "/"
                             + tableName).replace("//",
                                                  "/"));

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assertions.assertEquals(2, metadataVersionFiles(tableName).size());
    Assertions.assertEquals(0, manifestFiles(tableName).size());
  }


  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(),
                                                               renamedTableName);

    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assertions.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
    Assertions.assertTrue(catalog.tableExists(renameTableIdentifier));

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assertions.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
    Assertions.assertEquals(original.spec(), renamed.spec());
    Assertions.assertEquals(original.location(), renamed.location());
    Assertions.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());

    Assertions.assertTrue(catalog.dropTable(renameTableIdentifier));
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testDrop() {
    Assertions.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
    Assertions.assertTrue(catalog.dropTable(TABLE_IDENTIFIER));
    Assertions.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String fileLocation = table.location().replace("file:", "") + "/data/file.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
                                                       .schema(schema)
                                                       .named("test")
                                                       .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file = DataFiles.builder(table.spec())
                             .withRecordCount(3)
                             .withPath(fileLocation)
                             .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
                             .build();

    table.newAppend().appendFile(file).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assertions.assertTrue(catalog.dropTable(TABLE_IDENTIFIER, false));
    Assertions.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    Assertions.assertTrue(new File(fileLocation).exists());
    Assertions.assertTrue(new File(manifestListLocation).exists());
  }


  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String location1 = table.location().replace("file:", "") + "/data/file1.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location1))
                                                       .schema(schema)
                                                       .named("test")
                                                       .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    String location2 = table.location().replace("file:", "") + "/data/file2.avro";
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location2))
                                                       .schema(schema)
                                                       .named("test")
                                                       .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file1 = DataFiles.builder(table.spec())
                              .withRecordCount(3)
                              .withPath(location1)
                              .withFileSizeInBytes(Files.localInput(location2).getLength())
                              .build();

    DataFile file2 = DataFiles.builder(table.spec())
                              .withRecordCount(3)
                              .withPath(location2)
                              .withFileSizeInBytes(Files.localInput(location1).getLength())
                              .build();

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();

    Assertions.assertTrue(catalog.dropTable(TABLE_IDENTIFIER));
    Assertions.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));

    Assertions.assertFalse(new File(location1).exists());
    Assertions.assertFalse(new File(location2).exists());
    Assertions.assertFalse(new File(manifestListLocation).exists());
    for (ManifestFile manifest : manifests) {
      Assertions.assertFalse(new File(manifest.path().replace("file:", "")).exists());
    }
    Assertions.assertFalse(new File(
        ((HasTableOperations) table).operations()
                                  .current()
                                  .metadataFileLocation()
                                  .replace("file:", ""))
                             .exists());
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testExistingTableUpdate() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assertions.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assertions.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assertions.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testFailure() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    Branch branch = (Branch) client.getTreeApi().getReferenceByName(BRANCH);

    IcebergTable table = client.getContentsApi().getContents(BRANCH, KEY).unwrap(IcebergTable.class).get();

    client.getContentsApi().setContents(KEY, "random", PutContents.of(branch, IcebergTable.of("dummytable.metadata.json")));

    Assertions.assertThrows(CommitFailedException.class,
        () -> icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit());
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
                                                      .filter(t -> t.namespace()
                                                                    .level(0)
                                                                    .equals(DB_NAME)
                                                                   && t.name().equals(TABLE_NAME))
                                                      .collect(Collectors.toList());

    Assertions.assertEquals(1, expectedIdents.size());
    Assertions.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
  }

  private static String getTableBasePath(String tableName) {
    String databasePath = ALLEY_LOCAL_DIR.toString() + "/iceberg/warehouse/" + DB_NAME;
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  protected static Path getTableLocationPath(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString());
  }

  protected static String getTableLocation(String tableName) {
    return getTableLocationPath(tableName).toString();
  }

  private static String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private static List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
                 .map(File::getAbsolutePath)
                 .collect(Collectors.toList());
  }

  protected static List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
  }

  protected static List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private static List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName)
      .stream()
      .filter(f -> f.endsWith(extension))
      .collect(Collectors.toList());
  }

}
