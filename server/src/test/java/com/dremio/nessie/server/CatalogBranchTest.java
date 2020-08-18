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

import java.io.File;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.dremio.nessie.iceberg.NessieTableOperations;

@SuppressWarnings("MissingJavadocMethod")
public class CatalogBranchTest {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogBranchTest.class);
  private static TestNessieServer server;
  private static File alleyLocalDir;
  private NessieCatalog catalog;
  private NessieClient client;
  private Configuration hadoopConfig;

  @BeforeAll
  public static void create() throws Exception {
    try {
      SharedMetricRegistries.setDefault("default", new MetricRegistry());
    } catch (IllegalStateException t) {
      //pass set in previous test
    }
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
    String path = "http://localhost:9995/api/v1";
    String username = "test";
    String password = "test123";
    hadoopConfig.set("nessie.url", path);
    hadoopConfig.set("nessie.username", username);
    hadoopConfig.set("nessie.password", password);
    hadoopConfig.set("nessie.view-branch", "master");
    this.client = new NessieClient(AuthType.BASIC, path, username, password);
    catalog = new NessieCatalog(hadoopConfig);
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
    String initialMetadataLocation = getTag(catalog, foobar);
    Assertions
      .assertEquals(initialMetadataLocation, getTag(catalog, foobar));
    Assertions.assertEquals(getTag(newCatalog, foobaz), getTag(catalog, foobaz));
    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getTag(catalog, foobar), getTag(newCatalog, foobar));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getTag(newCatalog, foobar));
    initialMetadataLocation = getTag(newCatalog, foobaz);
    newCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getTag(catalog, foobaz), getTag(newCatalog, foobaz));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getTag(catalog, foobaz));

    Assertions.assertThrows(CommitFailedException.class,
        () -> newCatalog.promoteBranch("master", false));
    newCatalog.promoteBranch("master", true);
    Assertions.assertEquals(getTag(newCatalog, foobar),
                        getTag(catalog, foobar));
    Assertions.assertEquals(getTag(newCatalog, foobaz),
                        getTag(catalog, foobaz));
    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    catalog.dropBranch("test");
  }

  private static String getTag(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations alleyOps = (NessieTableOperations) ops;
    return alleyOps.currentMetadataLocation();
  }

  @AfterEach
  public void closeCatalog() throws Exception {
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

  private void createBranch(String name, String baseTag) {
    catalog.createBranch(name, baseTag);
  }

  private void createBranch(String name) {
    catalog.createBranch(name, "master");
  }

}
