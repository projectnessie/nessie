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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.security.TestSecurity;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;

class AbstractTestBasicOperations {

  private NessieApiV1 api;

  @AfterEach
  void closeClient() {
    if (api != null) {
      api.close();
      api = null;
    }
  }

  void getCatalog(String branch) throws BaseNessieClientServerException {
    api =
        HttpClientBuilder.builder()
            .withUri("http://localhost:19121/api/v1")
            .build(NessieApiV1.class);
    if (branch != null) {
      api.createReference().reference(Branch.of(branch, null)).sourceRefName("main").create();
    }
  }

  void tryEndpointPass(Executable runnable) {
    Assertions.assertDoesNotThrow(runnable);
  }

  @Test
  @TestSecurity(
      user = "admin_user",
      roles = {"admin", "user"})
  void testAdmin() throws BaseNessieClientServerException {
    getCatalog("testx");
    Branch branch = (Branch) api.getReference().refName("testx").get();
    List<Entry> tables = api.getEntries().refName("testx").get().getEntries();
    Assertions.assertTrue(tables.isEmpty());
    ContentsKey key = ContentsKey.of("x", "x");
    tryEndpointPass(
        () ->
            api.commitMultipleOperations()
                .branch(branch)
                .operation(Put.of(key, IcebergTable.of("foo", "x", "cid-foo")))
                .commitMeta(CommitMeta.fromMessage("empty message"))
                .commit());

    Assertions.assertTrue(
        api.getContents()
            .refName("testx")
            .key(key)
            .get()
            .get(key)
            .unwrap(IcebergTable.class)
            .isPresent());

    Branch master = (Branch) api.getReference().refName("testx").get();
    Branch test = Branch.of("testy", master.getHash());
    tryEndpointPass(
        () -> api.createReference().sourceRefName(master.getName()).reference(test).create());
    Branch test2 = (Branch) api.getReference().refName("testy").get();
    tryEndpointPass(() -> api.deleteBranch().branch(test2).delete());
    tryEndpointPass(
        () ->
            api.commitMultipleOperations()
                .branch(master)
                .operation(Delete.of(key))
                .commitMeta(CommitMeta.fromMessage(""))
                .commit());
    assertThat(api.getContents().refName("testx").key(key).get()).isEmpty();
    tryEndpointPass(
        () -> {
          Branch b = (Branch) api.getReference().refName(branch.getName()).get();
          // Note: the initial version-store implementations just committed this operation, but it
          // should actually fail, because the operations of the 1st commit above and this commit
          // have conflicts.
          api.commitMultipleOperations()
              .branch(b)
              .operation(Put.of(key, IcebergTable.of("bar", "x", "cid-bar")))
              .commitMeta(CommitMeta.fromMessage(""))
              .commit();
        });
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  void testUserCleanup() throws BaseNessieClientServerException {
    getCatalog(null);
    Branch r = (Branch) api.getReference().refName("testx").get();
    api.deleteBranch().branch(r).delete();
  }
}
