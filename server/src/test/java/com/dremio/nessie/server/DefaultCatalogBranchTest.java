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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.dremio.nessie.iceberg.NessieTableOperations;

/**
 * test tag operations with a default tag set by server.
 */
@SuppressWarnings("MissingJavadocMethod")
public class DefaultCatalogBranchTest {
  private static TestNessieServer server;
  private static File alleyLocalDir;
  private NessieCatalog catalog;
  private NessieClient client;
  private Configuration hadoopConfig;

  @BeforeAll
  public static void create() throws Exception {
    NessieTestServerBinder.settings.setDefaultTag("master");
    server = new TestNessieServer();
    server.start(9997);
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
    String path = "http://localhost:9997/api/v1";
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
    createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2

    hadoopConfig.set("nessie.view-branch", "FORWARD");
    NessieCatalog forwardCatalog = new NessieCatalog(hadoopConfig);
    forwardCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    forwardCatalog.loadTable(foobar).updateSchema().addColumn("id1", Types.LongType.get()).commit();
    Assertions.assertNotEquals(getTag(forwardCatalog, foobar),
                           getTag(catalog, foobar));
    Assertions.assertNotEquals(getTag(forwardCatalog, foobaz),
                           getTag(catalog, foobaz));

    forwardCatalog.refreshBranch();
    forwardCatalog.promoteBranch("master", false);

    Assertions.assertEquals(getTag(forwardCatalog, foobar),
                            getTag(catalog, foobar));
    Assertions.assertEquals(getTag(forwardCatalog, foobaz),
                        getTag(catalog, foobaz));

    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    catalog.dropBranch("FORWARD");
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
    return catalog.createTable(tableIdentifier, schema(count));
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
