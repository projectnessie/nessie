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

package org.projectnessie.server;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.projectnessie.server.TestUtils.meta;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.ClientContentsApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.rest.NessieForbiddenException;
import org.projectnessie.client.rest.NessieNotAuthorizedException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableBranch;
import org.projectnessie.model.Reference;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.security.TestSecurity;

@QuarkusTest
class TestAuth {

  private NessieClient client;
  private TreeApi tree;
  private ClientContentsApi contents;

  @AfterEach
  void closeClient() {
    if (client != null) {
      client.close();
      client = null;
    }
  }

  void getCatalog(String branch) throws NessieNotFoundException, NessieConflictException {
    client = NessieClient.none("http://localhost:19121/api/v1");
    tree = client.getTreeApi();
    contents = client.getContentsApi();
    if (branch != null) {
      tree.createReference(Branch.of(branch, null));
    }
  }

  void tryEndpointPass(Executable runnable) {
    Assertions.assertDoesNotThrow(runnable);
  }

  interface RunnableIO {
    public abstract void run() throws IOException;
  }

  void tryEndpointFail(Executable runnable) {
    Assertions.assertThrows(NessieForbiddenException.class, runnable);
  }

  @Disabled
  @Test
  void testLogin() {
    Assertions.assertThrows(NessieNotAuthorizedException.class, () -> getCatalog("x"));
  }

  @Test
  @TestSecurity(user = "admin_user", roles = {"admin", "user"})
  void testAdmin() throws NessieNotFoundException, NessieConflictException {
    getCatalog("testx");
    Branch branch = (Branch) tree.getReferenceByName("testx");
    List<Entry> tables = tree.getEntries("testx").getEntries();
    Assertions.assertTrue(tables.isEmpty());
    ContentsKey key = ContentsKey.of("x","x");
    tryEndpointPass(() -> contents.setContents(key, branch.getName(), branch.getHash(), meta("empty message"), IcebergTable.of("foo")));
    final IcebergTable table = contents.getContents(key, "testx").unwrap(IcebergTable.class).get();

    Branch master = (Branch) tree.getReferenceByName("testx");
    Branch test = ImmutableBranch.builder().hash(master.getHash()).name("testy").build();
    tryEndpointPass(() -> tree.createReference(Branch.of(test.getName(), test.getHash())));
    Branch test2 = (Branch) tree.getReferenceByName("testy");
    tryEndpointPass(() -> tree.deleteBranch(test2.getName(), test2.getHash()));
    tryEndpointPass(() -> contents.deleteContents(key, master.getName(), master.getHash(), meta("")));
    assertThrows(NessieNotFoundException.class, () -> contents.getContents(key, "testx"));
    tryEndpointPass(() -> contents.setContents(key, branch.getName(), branch.getHash(), meta(""), IcebergTable.of("bar")));
  }


  @Test
  @TestSecurity(authorizationEnabled = false)
  void testUserCleanup() throws NessieNotFoundException, NessieConflictException {
    getCatalog(null);
    Reference r = client.getTreeApi().getReferenceByName("testx");
    client.getTreeApi().deleteBranch(r.getName(), r.getHash());
  }

}
