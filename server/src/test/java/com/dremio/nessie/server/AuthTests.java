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

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class AuthTests {

  private static TestNessieServer server;

  private NessieClient client;

  @BeforeAll
  public static void create() throws Exception {
    server = new TestNessieServer();
    server.start(9993);
  }

  public void getCatalog(String username, String password) {
    String path = "http://localhost:9993";
    String base = "api/v1";
    this.client = new NessieClient(base, path, username, password);
  }

  @AfterAll
  public static void destroy() throws Exception {
    server.close();
    server = null;
  }

  public void tryEndpointPass(Runnable runnable) {
    try {
      runnable.run();
    } catch (Throwable t) {
      Assertions.fail();
    }
  }

  public void tryEndpointFail(Runnable runnable) {
    try {
      runnable.run();
      Assertions.fail();
    } catch (ForbiddenException e) {
      return;
    }
    Assertions.fail();
  }

  @Test
  public void testLogin() {
    try {
      getCatalog("x", "y");
      Assertions.fail();
    } catch (NotAuthorizedException e) {
      //what we expect
    } catch (Throwable t) {
      Assertions.fail();
    }
  }

  @Test
  public void testAdmin() {
    getCatalog("test", "test123");
    Branch branch = client.getBranch("master");
    Table[] tables = client.getAllTables("master", null);
    Assertions.assertEquals(0, tables.length);
    tryEndpointPass(() -> client.commit(branch, createTable("x", "x")));
    final Table table = client.getTable("master", "x", null);
    tryEndpointPass(() -> client.commit(branch, table));
    tryEndpointPass(() -> client.deleteTable(branch, table.getId(), false));
    Table newTable = client.getTable("master", table.getId(), null);
    Assertions.assertNull(newTable);
  }

  @Test
  public void testUser() {
    getCatalog("test", "test123");
    Branch branch = client.getBranch("master");
    tryEndpointPass(() -> client.commit(branch, createTable("x", "x")));
    getCatalog("normal", "hello123");
    final Table table = client.getTable("master", "x", null);
    Table[] tables = client.getAllTables("master", null);
    Assertions.assertEquals(1, tables.length);
    tryEndpointFail(() -> client.commit(branch, createTable("y", "x")));
    tryEndpointFail(() -> client.commit(branch, table));
    tryEndpointFail(() -> client.deleteTable(branch, "x", false));
    Table newTable = client.getTable("master", table.getId(), null);
    Assertions.assertNotNull(newTable);
    Assertions.assertEquals(table, newTable);
    getCatalog("test", "test123");
    tryEndpointPass(() -> client.deleteTable(branch, table.getId(), false));
  }

  private Table createTable(String name, String location) {
    return ImmutableTable.builder()
                         .tableName(name)
                         .namespace(location)
                         .metadataLocation("xxx")
                         .id(name)
                         .build();
  }
}
