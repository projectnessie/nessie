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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.iceberg.client.AlleyCatalog;
import com.dremio.iceberg.client.AlleyClient;
import com.dremio.iceberg.client.AlleyTableOperations;
import com.dremio.iceberg.client.tag.Tag;
import com.google.common.collect.Lists;

public class DefaultCatalogTagTest {

  private static TestAlleyServer server;
  private static File alleyLocalDir;
  private AlleyCatalog catalog;
  private AlleyClient client;
  private Configuration hadoopConfig;

  @BeforeClass
  public static void create() throws Exception {
    AlleyTestServerBinder.settings.setDefaultTag("HEAD");
    server = new TestAlleyServer();
    server.start(9998);
    alleyLocalDir = java.nio.file.Files.createTempDirectory("test",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"))).toFile();
  }

  @Before
  public void getCatalog() {
    hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", alleyLocalDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    hadoopConfig.set("iceberg.alley.host", "localhost");
    hadoopConfig.set("iceberg.alley.port", "9998");
    hadoopConfig.set("iceberg.alley.ssl", "false");
    hadoopConfig.set("iceberg.alley.base", "api/v1");
    hadoopConfig.set("iceberg.alley.username", "test");
    hadoopConfig.set("iceberg.alley.password", "test123");
    catalog = new AlleyCatalog(hadoopConfig);
    client = new AlleyClient(hadoopConfig);
  }

  @Test
  public void testBasicTag() {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2
    Tag forward = createTag("FORWARD");
    Tag head = createTag("HEAD");

    hadoopConfig.set("iceberg.alley.view-tag", "FORWARD");
    AlleyCatalog forwardCatalog = new AlleyCatalog(hadoopConfig);
    forwardCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    forwardCatalog.loadTable(foobar).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    Assert.assertEquals("HEAD", getTag(catalog, foobar).name());
    Assert.assertNotEquals(getTag(forwardCatalog, foobar).getMetadataLocation(foobar),
      getTag(catalog, foobar).getMetadataLocation(foobar));
    Assert.assertNotEquals(getTag(forwardCatalog, foobaz).getMetadataLocation(foobaz),
      getTag(catalog, foobaz).getMetadataLocation(foobaz));

    forward.refresh();
    head.copyTags().updateTag(forward).commit();
    Assert.assertEquals(getTag(forwardCatalog, foobar).getMetadataLocation(foobar),
      getTag(catalog, foobar).getMetadataLocation(foobar));
    Assert.assertEquals(getTag(forwardCatalog, foobaz).getMetadataLocation(foobaz),
      getTag(catalog, foobaz).getMetadataLocation(foobaz));

    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    catalog.dropTag("HEAD");
    catalog.dropTag("FORWARD");
  }

  private static Tag getTag(AlleyCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    AlleyTableOperations alleyOps = (AlleyTableOperations) ops;
    return alleyOps.currentTag();
  }

  @After
  public void closeCatalog() throws IOException {
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
  }

  @AfterClass
  public static void destroy() throws IOException {
    server = null;
  }

  private Table createTable(TableIdentifier tableIdentifier, int count) {
    return catalog.createTable(tableIdentifier, schema(count));
  }

  private static Schema schema(int count) {
    List<Types.NestedField> fields = Lists.newArrayList();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  private Tag createTag(String name, String baseTag) {
    return catalog.createTag(name, baseTag);
  }

  private Tag createTag(String name) {
    return catalog.createTag(name);
  }

}
