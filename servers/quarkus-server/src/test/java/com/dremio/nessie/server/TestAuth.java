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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.client.rest.NessieForbiddenException;
import com.dremio.nessie.client.rest.NessieNotAuthorizedException;
import com.dremio.nessie.client.rest.NessieNotFoundClientException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableBranch;
import com.dremio.nessie.model.ImmutableIcebergTable;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.ObjectsResponse.Entry;
import com.dremio.nessie.model.PutContents;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@QuarkusTest
class TestAuth {

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;

  void getCatalog(String branch) {
    String path = "http://localhost:19121/api/v1";
    this.client = new NessieClient(AuthType.NONE, path, null, null);
    tree = client.getTreeApi();
    contents = client.getContentsApi();
    if (branch != null) {
      tree.createNewReference(ImmutableBranch.builder().name(branch).build());
    }
  }

  void tryEndpointPass(Runnable runnable) {
    Assertions.assertDoesNotThrow(runnable::run);
  }

  void tryEndpointFail(Runnable runnable) {
    Assertions.assertThrows(NessieForbiddenException.class, runnable::run);
  }

  @Disabled
  @Test
  void testLogin() {
    Assertions.assertThrows(NessieNotAuthorizedException.class, () -> getCatalog("x"));
  }

  @Test
  @TestSecurity(user = "admin_user", roles = {"admin", "user"})
  void testAdmin() {
    getCatalog("testx");
    Branch branch = (Branch) tree.getReferenceByName("testx");
    List<Entry> tables = tree.getObjects("testx").getEntries();
    Assertions.assertTrue(tables.isEmpty());
    ContentsKey key = ContentsKey.of("x","x");
    tryEndpointPass(() -> contents.setContents(key, "foo", PutContents.of(branch, IcebergTable.of("foo"))));
    final IcebergTable table = contents.getContents("testx", key).unwrap(IcebergTable.class).get();

    Branch master = (Branch) tree.getReferenceByName("testx");
    Branch test = ImmutableBranch.builder().hash(master.getHash()).name("testy").build();
    tryEndpointPass(() -> tree.createNewReference(test));
    Branch test2 = (Branch) tree.getReferenceByName("testy");
    tryEndpointPass(() -> tree.deleteReference(test2));
    tryEndpointPass(() -> contents.deleteObject(key, "", master));
    assertThrows(NessieNotFoundClientException.class, () -> contents.getContents("testx", key));
    tryEndpointPass(() -> contents.setContents(key, "foo", PutContents.of(branch, IcebergTable.of("bar"))));
  }


  @Test
  @TestSecurity(authorizationEnabled = false)
  void testUserCleanup() {
    getCatalog(null);
    client.getTreeApi().deleteReference(client.getTreeApi().getReferenceByName("testx"));
  }

  private IcebergTable createTable(String name, String location) {
    return ImmutableIcebergTable.builder()
                         .metadataLocation("xxx")
                         .build();
  }
}
