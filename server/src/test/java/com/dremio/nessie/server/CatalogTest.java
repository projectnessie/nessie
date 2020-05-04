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

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.iceberg.NessieCatalog;
import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class CatalogTest {

  private static File alleyLocalDir;
  private static TestNessieServer server;
  private NessieCatalog catalog;
  private NessieClient client;

  @BeforeAll
  public static void create() throws Exception {
    server = new TestNessieServer();
    server.start(9996);
    alleyLocalDir = java.nio.file.Files.createTempDirectory("test",
                                                            PosixFilePermissions.asFileAttribute(
                                                              PosixFilePermissions.fromString(
                                                                "rwxrwxrwx"))).toFile();
  }

  @BeforeEach
  public void getCatalog() {
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", alleyLocalDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
                     org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    hadoopConfig.set("nessie.view-branch", "master");
    String base = "api/v1";
    String path = "http://localhost:9996";
    String username = "test";
    String password = "test123";
    hadoopConfig.set("nessie.url", path);
    hadoopConfig.set("nessie.base", base);
    hadoopConfig.set("nessie.username", username);
    hadoopConfig.set("nessie.password", password);
    hadoopConfig.set("nessie.view-branch", "master");
    this.client = new NessieClient(base, path, username, password);
    catalog = new NessieCatalog(hadoopConfig);
  }

  @Test
  public void test() {
    createTable(TableIdentifier.of("foo", "bar"));
    List<TableIdentifier> tables = catalog.listTables(Namespace.of("foo"));
    Assertions.assertEquals(1, tables.size());
    Assertions.assertEquals("bar", tables.get(0).name());
    Assertions.assertEquals("foo", tables.get(0).namespace().toString());
    catalog.renameTable(TableIdentifier.of("foo", "bar"), TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(null);
    Assertions.assertEquals(1, tables.size());
    Assertions.assertEquals("baz", tables.get(0).name());
    Assertions.assertEquals("foo", tables.get(0).namespace().toString());
    catalog.dropTable(TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(Namespace.empty());
    Assertions.assertTrue(tables.isEmpty());
  }

  @AfterEach
  public void closeCatalog() throws IOException {
    catalog.close();
    client.close();
    catalog = null;
    client = null;
  }

  @AfterAll
  public static void destroy() {
    server = null;
    alleyLocalDir.delete();
  }

  private void createTable(TableIdentifier tableIdentifier) {
    Schema schema = new Schema(StructType.of(required(1, "id", LongType.get()))
                                         .fields());
    catalog.createTable(tableIdentifier, schema).location();
  }

}
