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

import static org.apache.iceberg.types.Types.NestedField.required;

import com.dremio.nessie.client.NessieCatalog;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieTableOperations;
import com.dremio.nessie.client.branch.Branch;
import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogTagTest {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogTagTest.class);
  private static TestNessieServer server;
  private static File alleyLocalDir;
  private NessieCatalog catalog;
  private NessieClient client;
  private Configuration hadoopConfig;

  @BeforeAll
  public static void create() throws Exception {
    server = new TestNessieServer();
    server.start(9995);
    alleyLocalDir = java.nio.file.Files.createTempDirectory("test",
                                                            PosixFilePermissions.asFileAttribute(
                                                              PosixFilePermissions.fromString(
                                                                "rwxrwxrwx"))).toFile();
  }

  @BeforeEach
  public void getCatalog() {
    hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", alleyLocalDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
                     org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    hadoopConfig.set("nessie.url", "http://localhost:9995");
    hadoopConfig.set("nessie.base", "api/v1");
    hadoopConfig.set("nessie.username", "test");
    hadoopConfig.set("nessie.password", "test123");
    catalog = new NessieCatalog(hadoopConfig);
    client = new NessieClient(hadoopConfig);
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  public void testBasicTag() {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    Table bar = createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2
    createBranch("test");

    hadoopConfig.set("nessie.view-branch", "test");
    NessieCatalog newCatalog = new NessieCatalog(hadoopConfig);
    String initialMetadataLocation = getTag(catalog, foobar).getMetadataLocation(foobar);
    Assertions
      .assertEquals(initialMetadataLocation, getTag(catalog, foobar).getMetadataLocation(foobar));
    Assertions.assertEquals(getTag(newCatalog, foobaz).getMetadataLocation(foobaz),
                        getTag(catalog, foobaz).getMetadataLocation(foobaz));
    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getTag(catalog, foobar).getMetadataLocation(foobar),
                           getTag(newCatalog, foobar)
                             .getMetadataLocation(foobar));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation,
                        getTag(newCatalog, foobar)
                          .getMetadataLocation(foobar));
    initialMetadataLocation = getTag(newCatalog, foobaz).getMetadataLocation(foobaz);
    newCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertEquals(getTag(catalog, foobaz).getMetadataLocation(foobaz),
                        getTag(newCatalog, foobaz)
                          .getMetadataLocation(foobaz));

    // points to the previous metadata location
    Assertions.assertNotEquals(initialMetadataLocation,
                           getTag(catalog, foobaz)
                             .getMetadataLocation(foobaz));

    //newCatalog.loadBranch("test")
    //          .updateTables()
    //          .updateTable(foobar, ((BaseTable) catalog.loadTable(foobar)).operations())
    //          .commit();
    //Assertions.assertEquals(getTag(newCatalog, foobar).getMetadataLocation(foobar),
    //                    getTag(catalog, foobar).getMetadataLocation(foobar));
    //Assertions.assertEquals(getTag(newCatalog, foobaz).getMetadataLocation(foobaz),
    //                    getTag(catalog, foobaz).getMetadataLocation(foobaz));
    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    catalog.dropBranch("test");
  }

  private static Branch getTag(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations alleyOps = (NessieTableOperations) ops;
    return alleyOps.currentBranch();
  }

  @AfterEach
  public void closeCatalog() throws IOException {
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
  }

  @AfterAll
  public static void destroy() throws Exception {
    server.close();
    server = null;
  }

  private Table createTable(TableIdentifier tableIdentifier, int count) {
    try {
      return catalog.createTable(tableIdentifier, schema(count));
    } catch (Throwable t) {
      LOG.error("unable to do create " + tableIdentifier.toString(), t);
      throw t;
    }
  }

  private static Schema schema(int count) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  private Branch createBranch(String name, String baseTag) {
    return catalog.createBranch(name, baseTag);
  }

  private Branch createBranch(String name) {
    return catalog.createBranch(name);
  }

}
