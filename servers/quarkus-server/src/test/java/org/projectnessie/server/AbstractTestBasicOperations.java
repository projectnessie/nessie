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
import org.projectnessie.client.api.NessieApiVersion;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
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

  void getCatalog(String branch) throws NessieNotFoundException, NessieConflictException {
    api =
        HttpClientBuilder.builder()
            .withUri("http://localhost:19121/api/v1")
            .build(NessieApiVersion.V_1, NessieApiV1.class);
    if (branch != null) {
      api.createReference().reference(Branch.of(branch, null)).sourceRefName("main").submit();
    }
  }

  void tryEndpointPass(Executable runnable) {
    Assertions.assertDoesNotThrow(runnable);
  }

  @Test
  @TestSecurity(
      user = "admin_user",
      roles = {"admin", "user"})
  void testAdmin() throws NessieNotFoundException, NessieConflictException {
    getCatalog("testx");
    Branch branch = (Branch) api.getReference().refName("testx").submit();
    List<Entry> tables = api.getEntries().refName("testx").submit().getEntries();
    Assertions.assertTrue(tables.isEmpty());
    ContentsKey key = ContentsKey.of("x", "x");
    tryEndpointPass(
        () ->
            api.commitMultipleOperations()
                .branch(branch)
                .operation(Put.of(key, IcebergTable.of("foo", 42L, "cid-foo")))
                .commitMeta(CommitMeta.fromMessage("empty message"))
                .submit());

    Assertions.assertTrue(
        api.getContents()
            .refName("testx")
            .key(key)
            .submit()
            .get(key)
            .unwrap(IcebergTable.class)
            .isPresent());

    Branch master = (Branch) api.getReference().refName("testx").submit();
    Branch test = Branch.of("testy", master.getHash());
    tryEndpointPass(
        () -> api.createReference().sourceRefName(master.getName()).reference(test).submit());
    Branch test2 = (Branch) api.getReference().refName("testy").submit();
    tryEndpointPass(() -> api.deleteBranch().branch(test2).submit());
    tryEndpointPass(
        () ->
            api.commitMultipleOperations()
                .branch(master)
                .operation(Delete.of(key))
                .commitMeta(CommitMeta.fromMessage(""))
                .submit());
    assertThat(api.getContents().refName("testx").key(key).submit()).isEmpty();
    tryEndpointPass(
        () -> {
          Branch b = (Branch) api.getReference().refName(branch.getName()).submit();
          // Note: the initial version-store implementations just committed this operation, but it
          // should actually fail, because the operations of the 1st commit above and this commit
          // have conflicts.
          api.commitMultipleOperations()
              .branch(b)
              .operation(Put.of(key, IcebergTable.of("bar", 42L, "cid-bar")))
              .commitMeta(CommitMeta.fromMessage(""))
              .submit();
        });
  }

  @Test
  @TestSecurity(authorizationEnabled = false)
  void testUserCleanup() throws NessieNotFoundException, NessieConflictException {
    getCatalog(null);
    Branch r = (Branch) api.getReference().refName("testx").submit();
    api.deleteBranch().branch(r).submit();
  }
}
