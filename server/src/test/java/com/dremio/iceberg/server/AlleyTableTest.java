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
package com.dremio.iceberg.server;


import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.iceberg.client.AlleyCatalog;
import com.dremio.iceberg.client.AlleyClient;

public class AlleyTableTest {

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static TestAlleyServer server;
  private static File alleyLocalDir;

  private AlleyCatalog catalog;
  private AlleyClient client;
  private Path tableLocation;
  private org.apache.hadoop.conf.Configuration hadoopConfig;


  @BeforeClass
  public static void create() throws Exception {
    server = new TestAlleyServer();
    server.start(9991);
    alleyLocalDir = Files.createTempDirectory("test",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"))).toFile();
  }

  @Before
  public void getCatalog() {
    hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set("fs.defaultFS", alleyLocalDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    hadoopConfig.set("iceberg.alley.host", "localhost");
    hadoopConfig.set("iceberg.alley.port", "9991");
    hadoopConfig.set("iceberg.alley.ssl", "false");
    hadoopConfig.set("iceberg.alley.base", "api/v1");
    hadoopConfig.set("iceberg.alley.username", "admin_user");
    hadoopConfig.set("iceberg.alley.password", "test123");
    catalog = new AlleyCatalog(new com.dremio.iceberg.model.Configuration(hadoopConfig));
    client = new AlleyClient(hadoopConfig);

    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema()).location());

  }

  @After
  public void closeCatalog() throws IOException {
    // drop the table data
    tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
    catalog.dropTable(TABLE_IDENTIFIER, false /* metadata only, location was already deleted */);

    catalog.close();
    client.close();
    catalog = null;
    client = null;
  }

  @AfterClass
  public static void destroy() throws IOException {
    server = null;
    alleyLocalDir.delete();
  }

//  private void createTable(TableIdentifier tableIdentifier) {
//    org.apache.iceberg.alley.client.model.Table table = new org.apache.iceberg.alley.client.model
//    .Table(tableIdentifier.toString(), false);
//    webTarget.path("tables").request(MediaType.APPLICATION_JSON)
//        .post(Entity.entity(table, MediaType.APPLICATION_JSON));
//  }

  private com.dremio.iceberg.model.Table getTable(TableIdentifier tableIdentifier) {
    return client.getTable(tableIdentifier.name());
  }

  private static Schema schema() {
    return new Schema(
      Types.NestedField.required(1, "level", Types.StringType.get()),
      Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
      Types.NestedField.required(3, "message", Types.StringType.get()),
      Types.NestedField.optional(4, "call_stack",
        Types.ListType.ofRequired(5, Types.StringType.get()))
    );
  }

  @Test
  public void testCreate() {
    // Table should be created in hive metastore
    // Table should be renamed in hive metastore
    com.dremio.iceberg.model.Table table = getTable(TABLE_IDENTIFIER);
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    table = getTable(TABLE_IDENTIFIER);
    System.out.println(table);
    // check parameters are in expected state
//    Map<String, String> parameters = table.getParameters();
//    Assert.assertNotNull(parameters);
//    Assert.assertTrue(ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(parameters.get(TABLE_TYPE_PROP)));
//    Assert.assertTrue("EXTERNAL_TABLE".equalsIgnoreCase(table.getTableType()));
//
//    // Ensure the table is pointing to empty location
//    Assert.assertEquals(getTableLocation(tableName), table.getSd().getLocation());
//
//    // Ensure it is stored as unpartitioned table in hive.
//    Assert.assertEquals(0, table.getPartitionKeysSize());
//
//    // Only 1 snapshotFile Should exist and no manifests should exist
//    Assert.assertEquals(1, metadataVersionFiles(tableName).size());
//    Assert.assertEquals(0, manifestFiles(tableName).size());
//
//    final Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
//    // Iceberg schema should match the loaded table
//    Assert.assertEquals(schema.asStruct(), icebergTable.schema().asStruct());
  }
//
//  @Test
//  public void testRename() {
//    String renamedTableName = "rename_table_name";
//    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);
//    Table original = catalog.loadTable(TABLE_IDENTIFIER);
//
//    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
//    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
//    Assert.assertTrue(catalog.tableExists(renameTableIdentifier));
//
//    Table renamed = catalog.loadTable(renameTableIdentifier);
//
//    Assert.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
//    Assert.assertEquals(original.spec(), renamed.spec());
//    Assert.assertEquals(original.location(), renamed.location());
//    Assert.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());
//
//    Assert.assertTrue(catalog.dropTable(renameTableIdentifier));
//  }
//
//  @Test
//  public void testDrop() {
//    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));
//    Assert.assertTrue("Drop should return true and drop the table", catalog.dropTable(TABLE_IDENTIFIER));
//    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
//  }
//
//  @Test
//  public void testDropWithoutPurgeLeavesTableData() throws IOException {
//    Table table = catalog.loadTable(TABLE_IDENTIFIER);
//
//    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
//    List<GenericData.Record> records = Lists.newArrayList(
//        recordBuilder.set("id", 1L).build(),
//        recordBuilder.set("id", 2L).build(),
//        recordBuilder.set("id", 3L).build()
//    );
//
//    String fileLocation = table.location().replace("file:", "") + "/data/file.avro";
//    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
//        .schema(schema)
//        .named("test")
//        .build()) {
//      for (GenericData.Record rec : records) {
//        writer.add(rec);
//      }
//    }
//
//    DataFile file = DataFiles.builder(table.spec())
//        .withRecordCount(3)
//        .withPath(fileLocation)
//        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
//        .build();
//
//    table.newAppend().appendFile(file).commit();
//
//    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");
//
//    Assert.assertTrue("Drop should return true and drop the table",
//        catalog.dropTable(TABLE_IDENTIFIER, false /* do not delete underlying files */));
//    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
//
//    Assert.assertTrue("Table data files should exist",
//        new File(fileLocation).exists());
//    Assert.assertTrue("Table metadata files should exist",
//        new File(manifestListLocation).exists());
//  }
//
//  @Test
//  public void testDropTable() throws IOException {
//    Table table = catalog.loadTable(TABLE_IDENTIFIER);
//
//    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
//    List<GenericData.Record> records = Lists.newArrayList(
//        recordBuilder.set("id", 1L).build(),
//        recordBuilder.set("id", 2L).build(),
//        recordBuilder.set("id", 3L).build()
//    );
//
//    String location1 = table.location().replace("file:", "") + "/data/file1.avro";
//    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location1))
//        .schema(schema)
//        .named("test")
//        .build()) {
//      for (GenericData.Record rec : records) {
//        writer.add(rec);
//      }
//    }
//
//    String location2 = table.location().replace("file:", "") + "/data/file2.avro";
//    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(location2))
//        .schema(schema)
//        .named("test")
//        .build()) {
//      for (GenericData.Record rec : records) {
//        writer.add(rec);
//      }
//    }
//
//    DataFile file1 = DataFiles.builder(table.spec())
//        .withRecordCount(3)
//        .withPath(location1)
//        .withFileSizeInBytes(Files.localInput(location2).getLength())
//        .build();
//
//    DataFile file2 = DataFiles.builder(table.spec())
//        .withRecordCount(3)
//        .withPath(location2)
//        .withFileSizeInBytes(Files.localInput(location1).getLength())
//        .build();
//
//    // add both data files
//    table.newAppend().appendFile(file1).appendFile(file2).commit();
//
//    // delete file2
//    table.newDelete().deleteFile(file2.path()).commit();
//
//    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");
//
//    List<ManifestFile> manifests = table.currentSnapshot().manifests();
//
//    Assert.assertTrue("Drop (table and data) should return true and drop the table",
//        catalog.dropTable(TABLE_IDENTIFIER));
//    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
//
//    Assert.assertFalse("Table data files should not exist",
//        new File(location1).exists());
//    Assert.assertFalse("Table data files should not exist",
//        new File(location2).exists());
//    Assert.assertFalse("Table manifest list files should not exist",
//        new File(manifestListLocation).exists());
//    for (ManifestFile manifest : manifests) {
//      Assert.assertFalse("Table manifest files should not exist",
//          new File(manifest.path().replace("file:", "")).exists());
//    }
//    Assert.assertFalse("Table metadata file should not exist",
//        new File(((HasTableOperations) table).operations().current().file().location().replace("file:", "")).exists())
//  }
//
//  @Test
//  public void testExistingTableUpdate() throws TException {
//    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
//    // add a column
//    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();
//
//    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
//
//    // Only 2 snapshotFile Should exist and no manifests should exist
//    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
//    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
//    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());
//
//    final org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
//    final List<String> hiveColumns = table.getSd().getCols().stream()
//        .map(FieldSchema::getName)
//        .collect(Collectors.toList());
//    final List<String> icebergColumns = altered.columns().stream()
//        .map(Types.NestedField::name)
//        .collect(Collectors.toList());
//    Assert.assertEquals(icebergColumns, hiveColumns);
//  }
//
//  @Test(expected = CommitFailedException.class)
//  public void testFailure() throws TException {
//    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
//    org.apache.hadoop.hive.metastore.api.Table table = metastoreClient.getTable(DB_NAME, TABLE_NAME);
//    String dummyLocation = "dummylocation";
//    table.getParameters().put(METADATA_LOCATION_PROP, dummyLocation);
//    metastoreClient.alter_table(DB_NAME, TABLE_NAME, table);
//    icebergTable.updateSchema()
//        .addColumn("data", Types.LongType.get())
//        .commit();
//  }
//
//  @Test
//  public void testListTables() {
//    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
//    List<TableIdentifier> expectedIdents = tableIdents.stream()
//        .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
//        .collect(Collectors.toList());
//
//    Assert.assertEquals(1, expectedIdents.size());
//    Assert.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
//  }
}
