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

import com.dremio.iceberg.client.AlleyClient;
import com.dremio.iceberg.model.Table;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AuthTests {

  private static TestAlleyServer server;

  private AlleyClient client;
  private org.apache.hadoop.conf.Configuration hadoopConfig;


  @BeforeClass
  public static void create() throws Exception {
    server = new TestAlleyServer();
    server.start(9993);
  }

  public void getCatalog(String username, String password) {
    hadoopConfig = new org.apache.hadoop.conf.Configuration();
    hadoopConfig.set("iceberg.alley.host", "localhost");
    hadoopConfig.set("iceberg.alley.port", "9993");
    hadoopConfig.set("iceberg.alley.ssl", "false");
    hadoopConfig.set("iceberg.alley.base", "api/v1");
    hadoopConfig.set("iceberg.alley.username", username);
    hadoopConfig.set("iceberg.alley.password", password);
    client = new AlleyClient(hadoopConfig);
  }

  @AfterClass
  public static void destroy() throws Exception {
    server.close();
    server = null;
  }

  public void tryEndpointPass(Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      Assert.fail();
    }
  }

  public void tryEndpointFail(Runnable runnable) {
    try {
      runnable.run();
      Assert.fail();
    } catch (ForbiddenException e) {
      return;
    }
    Assert.fail();
  }

  @Test
  public void testLogin() {
    try {
      getCatalog("x", "y");
      Assert.fail();
    } catch (NotAuthorizedException e) {
      //what we expect
    } catch (Throwable t) {
      Assert.fail();
    }
  }

  @Test
  public void testAdmin() {
    getCatalog("test", "test123");
    Table[] tables = client.getTableClient().getAll();
    Assert.assertEquals(0, tables.length);
    tryEndpointPass(() -> client.getTableClient().createObject(createTable("x", "x")));
    final Table table = client.getTableClient().getObjectByName("x", null);
    tryEndpointPass(() -> client.getTableClient().updateObject(table));
    tryEndpointPass(() -> client.getTableClient().deleteObject(table.getId(), false));
    Table newTable = client.getTableClient().getObject(table.getId());
    Assert.assertNull(newTable);
  }

  @Test
  public void testUser() {
    getCatalog("test", "test123");
    tryEndpointPass(() -> client.getTableClient().createObject(createTable("x", "x")));
    getCatalog("normal", "hello123");
    final Table table = client.getTableClient().getObjectByName("x", null);
    Table[] tables = client.getTableClient().getAll();
    Assert.assertEquals(1, tables.length);
    tryEndpointFail(() -> client.getTableClient().createObject(createTable("y", "x")));
    tryEndpointFail(() -> client.getTableClient().updateObject(table));
    tryEndpointFail(() -> client.getTableClient().deleteObject("x", false));
    Table newTable = client.getTableClient().getObject(table.getId());
    Assert.assertNotNull(newTable);
    Assert.assertEquals(table, newTable);
  }

  private Table createTable(String name, String location) {
    return Table.builder()
                .tableName(name)
                .baseLocation(location)
                .metadataLocation("xxx")
                .build();
  }
}
