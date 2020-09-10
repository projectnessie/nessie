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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.rest.NessieForbiddenException;
import com.dremio.nessie.client.rest.NessieNotAuthorizedException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableTable;
import com.dremio.nessie.model.Table;
import com.google.common.collect.Lists;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@SuppressWarnings("MissingJavadocMethod")
@QuarkusTest
public class AuthTests {

  private NessieClient client;

  public void getCatalog(String branch) {
    String path = "http://localhost:19121/api/v1";
    this.client = new NessieClient(AuthType.NONE, path, null, null);
    if (branch != null) {
      client.createBranch(ImmutableBranch.builder().name(branch).build());
    }
  }

  public void tryEndpointPass(Runnable runnable) {
    Assertions.assertDoesNotThrow(runnable::run);
  }

  public void tryEndpointFail(Runnable runnable) {
    Assertions.assertThrows(NessieForbiddenException.class, runnable::run);
  }

  @Test
  public void testLogin() {
    Assertions.assertThrows(NessieNotAuthorizedException.class, () -> getCatalog("x"));
  }

  @Test
  @TestSecurity(user = "admin_user", roles = {"admin", "user"})
  public void testAdmin() {
    getCatalog("testx");
    Branch branch = client.getBranch("testx");
    Iterable<String> tables = client.getAllTables("testx", null);
    Assertions.assertTrue(Lists.newArrayList(tables).isEmpty());
    tryEndpointPass(() -> client.commit(branch, createTable("x", "x")));
    final Table table = client.getTable("testx", "x", null);
    tryEndpointPass(() -> client.commit(branch, table));
    Branch master = client.getBranch("testx");
    Branch test = ImmutableBranch.builder().id(master.getId()).name("testy").build();
    tryEndpointPass(() -> client.createBranch(test));
    Branch test2 = client.getBranch("testy");
    tryEndpointPass(() -> client.deleteBranch(test2));
    tryEndpointPass(() -> client.commit(branch, ImmutableTable.copyOf(table).withIsDeleted(true)));
    Table newTable = client.getTable("testx", table.getId(), null);
    Assertions.assertNull(newTable);
    tryEndpointPass(() -> client.commit(branch, createTable("x", "x")));
  }

  @Test
  @TestSecurity(user = "testUser", roles = {"user"})
  public void testUser() {
    getCatalog(null);
    Branch branch = client.getBranch("testx");
    Assertions.assertThrows(NessieForbiddenException.class, () -> getCatalog("normalx"));
    final Table table = client.getTable("testx", "x", null);
    Iterable<String> tables = client.getAllTables("testx", null);
    Assertions.assertEquals(1, Lists.newArrayList(tables).size());
    tryEndpointFail(() -> client.commit(branch, createTable("y", "x")));
    tryEndpointFail(() -> client.commit(branch, table));
    tryEndpointFail(() -> client.createBranch(branch));
    tryEndpointFail(() -> client.deleteBranch(branch));
    Table newTable = client.getTable("testx", table.getId(), null);
    Assertions.assertNotNull(newTable);
    Assertions.assertEquals(table, newTable);
    tryEndpointFail(() -> client.commit(branch, ImmutableTable.copyOf(table).withIsDeleted(true)));
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
