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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.iceberg.NessieCatalog;
import com.dremio.nessie.iceberg.NessieTableOperations;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.Reference;

abstract class BaseTestIceberg {

  protected final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

  protected static File ALLEY_LOCAL_DIR;
  protected NessieCatalog catalog;
  protected NessieClient client;
  protected TreeApi tree;
  protected ContentsApi contents;
  protected Configuration hadoopConfig;
  protected final String branch;

  @BeforeAll
  public static void create() throws Exception {
    ALLEY_LOCAL_DIR = java.nio.file.Files.createTempDirectory(
        "test",
        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx")))
        .toFile();
  }

  public BaseTestIceberg(String branch) {
    super();
    this.branch = branch;
  }

  private void resetData() {
    for (Reference r : tree.getAllReferences()) {
      tree.deleteReference(r);
    }
    client.getTreeApi().createNewReference(ImmutableBranch.builder().name("main").build());
  }

  @BeforeEach
  public void beforeEach() {
    resetData();

    hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", ALLEY_LOCAL_DIR.toURI().toString());
    hadoopConfig.set("fs.file.impl",
                     org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    String path = "http://localhost:19121/api/v1";
    String username = "test";
    String password = "test123";
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_URL, path);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_USERNAME, username);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_PASSWORD, password);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_REF, branch);
    hadoopConfig.set(NessieCatalog.CONF_NESSIE_AUTH_TYPE, "NONE");
    this.client = new NessieClient(AuthType.NONE, path, username, password);
    tree = client.getTreeApi();
    contents = client.getContentsApi();
    catalog = new NessieCatalog(hadoopConfig);
    try {
      tree.createNewReference(ImmutableBranch.builder().name(branch).build());
    } catch (Exception e) {
      //ignore, already created. Cant run this in BeforeAll as quarkus hasn't disabled auth
    }
  }

  protected Table createTable(TableIdentifier tableIdentifier, int count) {
    try {
      return catalog.createTable(tableIdentifier, schema(count));
    } catch (Throwable t) {
      LOGGER.error("unable to do create " + tableIdentifier.toString(), t);
      throw t;
    }
  }

  protected static Schema schema(int count) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(required(i, "id" + i, Types.LongType.get()));
    }
    return new Schema(Types.StructType.of(fields).fields());
  }

  void createBranch(String name, String hash) {
    catalog.createBranch(name, Optional.ofNullable(hash));
  }

  @AfterEach
  public void afterEach() throws Exception {
    client.getTreeApi().deleteReference(client.getTreeApi().getReferenceByName("main"));
    catalog.close();
    client.close();
    catalog = null;
    client = null;
    hadoopConfig = null;
  }

  @AfterAll
  public static void destroy() throws Exception {
    ALLEY_LOCAL_DIR.delete();
  }

  static String getBranch(NessieCatalog catalog, TableIdentifier tableIdentifier) {
    Table table = catalog.loadTable(tableIdentifier);
    BaseTable baseTable = (BaseTable) table;
    TableOperations ops = baseTable.operations();
    NessieTableOperations alleyOps = (NessieTableOperations) ops;
    return alleyOps.currentMetadataLocation();
  }

}
