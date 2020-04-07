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


import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.iceberg.client.AlleyCatalog;
import com.dremio.iceberg.client.AlleyClient;
import com.google.common.collect.Lists;

public class AlleyTableTest {

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static TestAlleyServer server;
  private static File alleyLocalDir;
  private static final Schema schema = new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final Schema altered = new Schema(Types.StructType.of(
    required(1, "id", Types.LongType.get()),
    optional(2, "data", Types.LongType.get())).fields());

  private AlleyCatalog catalog;
  private AlleyClient client;
  private Path tableLocation;
  private org.apache.hadoop.conf.Configuration hadoopConfig;


  @BeforeClass
  public static void create() throws Exception {
    server = new TestAlleyServer();
    server.start(9991);
    alleyLocalDir = java.nio.file.Files.createTempDirectory("test",
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

    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());

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
  public static void destroy() {
    server = null;
    alleyLocalDir.delete();
  }

  private com.dremio.iceberg.model.Table getTable(TableIdentifier tableIdentifier) {
    return client.getTableByName(tableIdentifier.name(), tableIdentifier.namespace().toString());
  }

  @Test
  public void testCreate() {
    // Table should be created in alley
    // Table should be renamed in alley
    String tableName = TABLE_IDENTIFIER.name();
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    com.dremio.iceberg.model.Table table = getTable(TABLE_IDENTIFIER);
    // check parameters are in expected state
    Assert.assertEquals(getTableLocation(tableName),
      (table.getBaseLocation() + "/" + DB_NAME + "/" + tableName).replace("//", "/"));

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(tableName).size());
    Assert.assertEquals(0, manifestFiles(tableName).size());
  }


  @Test
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);
    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assert.assertFalse(catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue(catalog.tableExists(renameTableIdentifier));

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assert.assertEquals(original.schema().asStruct(), renamed.schema().asStruct());
    Assert.assertEquals(original.spec(), renamed.spec());
    Assert.assertEquals(original.location(), renamed.location());
    Assert.assertEquals(original.currentSnapshot(), renamed.currentSnapshot());

    Assert.assertTrue(catalog.dropTable(renameTableIdentifier));
  }

  @Test
  public void testDrop() {
    Assert.assertTrue("Table should exist", catalog.tableExists(TABLE_IDENTIFIER));
    Assert.assertTrue("Drop should return true and drop the table", catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
      recordBuilder.set("id", 1L).build(),
      recordBuilder.set("id", 2L).build(),
      recordBuilder.set("id", 3L).build()
    );

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

    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assert.assertTrue("Drop should return true and drop the table",
      catalog.dropTable(TABLE_IDENTIFIER, false /* do not delete underlying files */));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertTrue("Table data files should exist",
      new File(fileLocation).exists());
    Assert.assertTrue("Table metadata files should exist",
      new File(manifestListLocation).exists());
  }

  @Test
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayList(
      recordBuilder.set("id", 1L).build(),
      recordBuilder.set("id", 2L).build(),
      recordBuilder.set("id", 3L).build()
    );

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

    String manifestListLocation = table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().manifests();

    Assert.assertTrue("Drop (table and data) should return true and drop the table",
      catalog.dropTable(TABLE_IDENTIFIER));
    Assert.assertFalse("Table should not exist", catalog.tableExists(TABLE_IDENTIFIER));

    Assert.assertFalse("Table data files should not exist",
      new File(location1).exists());
    Assert.assertFalse("Table data files should not exist",
      new File(location2).exists());
    Assert.assertFalse("Table manifest list files should not exist",
      new File(manifestListLocation).exists());
    for (ManifestFile manifest : manifests) {
      Assert.assertFalse("Table manifest files should not exist",
        new File(manifest.path().replace("file:", "")).exists());
    }
    Assert.assertFalse("Table metadata file should not exist",
      new File(((HasTableOperations) table).operations().current().file().location().replace("file:", "")).exists());
  }

  @Test
  public void testExistingTableUpdate() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assert.assertEquals(2, metadataVersionFiles(TABLE_NAME).size());
    Assert.assertEquals(0, manifestFiles(TABLE_NAME).size());
    Assert.assertEquals(altered.asStruct(), icebergTable.schema().asStruct());

  }

  @Test(expected = CommitFailedException.class)
  public void testFailure() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    com.dremio.iceberg.model.Table table = client.getTableByName(TABLE_NAME, DB_NAME);
    client.updateTable(table.newMetadataLocation("dummytable"));
    icebergTable.updateSchema()
      .addColumn("data", Types.LongType.get())
      .commit();
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
      .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
      .collect(Collectors.toList());

    Assert.assertEquals(1, expectedIdents.size());
    Assert.assertTrue(catalog.tableExists(TABLE_IDENTIFIER));
  }

  private static String getTableBasePath(String tableName) {
    String databasePath = alleyLocalDir.toString() + "/iceberg/warehouse/" + DB_NAME;
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
