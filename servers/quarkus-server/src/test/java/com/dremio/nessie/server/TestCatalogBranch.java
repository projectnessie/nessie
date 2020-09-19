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
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.dremio.nessie.iceberg.NessieTableOperations;
import com.dremio.nessie.model.ImmutableBranch;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@SuppressWarnings("MissingJavadocMethod")
@QuarkusTest
public class TestCatalogBranch {

  private static final Logger LOG = LoggerFactory.getLogger(TestCatalogBranch.class);
  private static File alleyLocalDir;
  private NessieCatalog catalog;
  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private Configuration hadoopConfig;

  @BeforeAll
  public static void create() throws Exception {
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
    String path = "http://localhost:19121/api/v1";
    String username = "test";
    String password = "test123";
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_URL, path);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_USERNAME, username);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_PASSWORD, password);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_REF, "main");
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_AUTH_TYPE, "NONE");
    this.client = new NessieClient(AuthType.NONE, path, username, password);
    tree = client.getTreeApi();
    contents = client.getContentsApi();
    catalog = new NessieCatalog(hadoopConfig);
    try {
      tree.createNewReference(ImmutableBranch.builder().name("main").build());
    } catch (Exception e) {
      //ignore, already created. Cant run this in BeforeAll as quarkus hasn't disabled auth
    }
  }

  @SuppressWarnings("VariableDeclarationUsageDistance")
  @Test
  @TestSecurity(authorizationEnabled = false)
  public void testBasicBranch() {
    TableIdentifier foobar = TableIdentifier.of("foo", "bar");
    TableIdentifier foobaz = TableIdentifier.of("foo", "baz");
    Table bar = createTable(foobar, 1); //table 1
    createTable(foobaz, 1); //table 2
    createBranch("test", catalog.getHash());

    hadoopConfig.set(NessieCatalog.CONF_NESSIE_REF, catalog.getHash());

    NessieCatalog newCatalog = new NessieCatalog(hadoopConfig);
    String initialMetadataLocation = getBranch(newCatalog, foobar);
    Assertions.assertEquals(initialMetadataLocation, getBranch(catalog, foobar));
    Assertions.assertEquals(getBranch(newCatalog, foobaz), getBranch(catalog, foobaz));
    bar.updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getBranch(catalog, foobar), getBranch(newCatalog, foobar));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getBranch(newCatalog, foobar));
    initialMetadataLocation = getBranch(newCatalog, foobaz);
    newCatalog.loadTable(foobaz).updateSchema().addColumn("id1", Types.LongType.get()).commit();

    // metadata location changed no longer matches
    Assertions.assertNotEquals(getBranch(catalog, foobaz), getBranch(newCatalog, foobaz));

    // points to the previous metadata location
    Assertions.assertEquals(initialMetadataLocation, getBranch(catalog, foobaz));

    newCatalog.assignReference("main", client.getTreeApi().getReferenceByName("main").getHash(), newCatalog.getHash());
    Assertions.assertEquals(getBranch(newCatalog, foobar),
                            getBranch(catalog, foobar));
    Assertions.assertEquals(getBranch(newCatalog, foobaz),
                            getBranch(catalog, foobaz));
    catalog.dropTable(foobar);
    catalog.dropTable(foobaz);
    catalog.deleteBranch("test", catalog.getHash());
  }

  private static String getBranch(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations alleyOps = (NessieTableOperations) ops;
    return alleyOps.currentMetadataLocation();
  }

  @AfterEach
  public void closeCatalog() throws Exception {
    client.getTreeApi().deleteReference(client.getTreeApi().getReferenceByName("main"));
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
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

  private void createBranch(String name, String hash) {
    catalog.createBranch(name, Optional.ofNullable(hash));
  }

}
