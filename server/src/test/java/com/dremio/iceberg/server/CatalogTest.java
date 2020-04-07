/*
 * Copyright (C) 2020 Dremio
 *
 *             Licensed under the Apache License, Version 2.0 (the "License");
 *             you may not use this file except in compliance with the License.
 *             You may obtain a copy of the License at
 *
 *             http://www.apache.org/licenses/LICENSE-2.0
 *
 *             Unless required by applicable law or agreed to in writing, software
 *             distributed under the License is distributed on an "AS IS" BASIS,
 *             WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *             See the License for the specific language governing permissions and
 *             limitations under the License.
 */

package com.dremio.iceberg.server;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.iceberg.client.AlleyCatalog;
import com.dremio.iceberg.model.Table;

public class CatalogTest {

  private static TestAlleyServer server;
  private static File alleyLocalDir;
  private AlleyCatalog catalog;
  private Client client;
  private WebTarget webTarget;
  private String authHeader;

  @BeforeClass
  public static void create() throws Exception {
    server = new TestAlleyServer();
    server.start();
    alleyLocalDir = Files.createTempDirectory("test",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxrwxrwx"))).toFile();
  }

  @Before
  public void getCatalog() {
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.set("fs.defaultFS", alleyLocalDir.toURI().toString());
    hadoopConfig.set("fs.file.impl",
      org.apache.hadoop.fs.LocalFileSystem.class.getName()
    );
    hadoopConfig.set("iceberg.alley.host", "localhost");
    hadoopConfig.set("iceberg.alley.port", "19120");
    hadoopConfig.set("iceberg.alley.ssl", "false");
    hadoopConfig.set("iceberg.alley.base", "api/v1");
    hadoopConfig.set("iceberg.alley.username", "admin_user");
    hadoopConfig.set("iceberg.alley.password", "test123");
    catalog = new AlleyCatalog(new com.dremio.iceberg.model.Configuration(hadoopConfig));
    client = ClientBuilder.newClient(); //todo create this on the fly rather than keeping it open. with a try block
    webTarget = client.target("http://localhost:19120/api/v1"); // todo config driven & safety tests
    MultivaluedMap<String, String> creds = new MultivaluedHashMap();
    creds.add("username", "admin_user");
    creds.add("password", "test123");
    webTarget = client.target("http://localhost:19120/api/v1"); // todo config driven & safety tests
    Response response = webTarget.path("login").request(MediaType.APPLICATION_FORM_URLENCODED).post(Entity.form(creds));
    authHeader = response.getHeaderString(HttpHeaders.AUTHORIZATION);
  }

  @Test
  public void test() {
    createTable(TableIdentifier.of("foo", "bar"));
    List<TableIdentifier> tables = catalog.listTables(Namespace.empty());
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TableIdentifier.of("foo", "bar"), tables.get(0));
    catalog.renameTable(TableIdentifier.of("foo", "bar"), TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(Namespace.empty());
    Assert.assertEquals(1, tables.size());
    Assert.assertEquals(TableIdentifier.of("foo", "baz"), tables.get(0));
    catalog.dropTable(TableIdentifier.of("foo", "baz"));
    tables = catalog.listTables(Namespace.empty());
    Assert.assertTrue(tables.isEmpty());
  }

  @After
  public void closeCatalog() throws IOException {
    catalog.close();
    client.close();
    catalog = null;
    webTarget = null;
    client = null;
    authHeader = null;
  }

  @AfterClass
  public static void destroy() throws IOException {
    server = null;
  }

  private void createTable(TableIdentifier tableIdentifier) {
    Table table = new Table(tableIdentifier.toString(), null);
    webTarget.path("tables").request(MediaType.APPLICATION_JSON).header(HttpHeaders.AUTHORIZATION, authHeader)
      .post(Entity.entity(table, MediaType.APPLICATION_JSON));
  }

}
