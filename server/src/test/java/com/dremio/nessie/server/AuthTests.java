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

import javax.ws.rs.ForbiddenException;
import javax.ws.rs.NotAuthorizedException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import com.google.common.collect.Lists;

@SuppressWarnings("MissingJavadocMethod")
public class AuthTests {

  private static TestNessieServer server;

  private NessieClient client;

  @BeforeAll
  public static void create() throws Exception {
    try {
      SharedMetricRegistries.setDefault("default", new MetricRegistry());
    } catch (IllegalStateException t) {
      //pass set in previous test
    }
    server = new TestNessieServer();
    server.start(9993);
  }

  public void getCatalog(String username, String password) {
    String path = "http://localhost:9993/api/v1";
    this.client = new NessieClient(AuthType.BASIC, path, username, password);
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
    Iterable<String> tables = client.getAllTables("master", null);
    Assertions.assertTrue(Lists.newArrayList(tables).isEmpty());
    tryEndpointPass(() -> client.commit(branch, createTable("x", "x")));
    final Table table = client.getTable("master", "x", null);
    tryEndpointPass(() -> client.commit(branch, table));
    Branch test = ImmutableBranch.builder().id("master").name("test").build();
    tryEndpointPass(() -> client.createBranch(test));
    Branch test2 = client.getBranch("test");
    tryEndpointPass(() -> client.deleteBranch(test2));
    tryEndpointPass(() -> client.commit(branch, ImmutableTable.copyOf(table).withIsDeleted(true)));
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
    Iterable<String> tables = client.getAllTables("master", null);
    Assertions.assertEquals(1, Lists.newArrayList(tables).size());
    tryEndpointFail(() -> client.commit(branch, createTable("y", "x")));
    tryEndpointFail(() -> client.commit(branch, table));
    tryEndpointFail(() -> client.createBranch(branch));
    tryEndpointFail(() -> client.deleteBranch(branch));
    Table newTable = client.getTable("master", table.getId(), null);
    Assertions.assertNotNull(newTable);
    Assertions.assertEquals(table, newTable);
    getCatalog("test", "test123");
    tryEndpointPass(() -> client.commit(branch, ImmutableTable.copyOf(table).withIsDeleted(true)));
  }

  private Table createTable(String name, String location) {
    return ImmutableTable.builder()
                         .name(name)
                         .namespace(location)
                         .metadataLocation("xxx")
                         .id(name)
                         .build();
  }
}
