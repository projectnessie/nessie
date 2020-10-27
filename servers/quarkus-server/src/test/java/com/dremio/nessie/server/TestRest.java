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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.MultiGetContentsRequest;
import com.dremio.nessie.model.MultiGetContentsResponse.ContentsWithKey;
import com.dremio.nessie.model.Reference;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest {

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;

  @BeforeEach
  void init() throws NessieNotFoundException, NessieConflictException {
    String path = "http://localhost:19121/api/v1";
    this.client = new NessieClient(AuthType.NONE, path, null, null);
    tree = client.getTreeApi();
    contents = client.getContentsApi();
  }

  @Test
  void multiget() throws NessieNotFoundException, NessieConflictException {
    final String branch = "foo";
    tree.createReference(Branch.of(branch, null));
    Reference r = tree.getReferenceByName(branch);
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    IcebergTable tb = IcebergTable.of("path2");
    contents.setContents(a, branch, r.getHash(), "commit 1", ta);
    contents.setContents(b, branch, r.getHash(), "commit 2", tb);
    List<ContentsWithKey> keys =
        contents.getMultipleContents("foo", MultiGetContentsRequest.of(a, b, ContentsKey.of("noexist"))).getContents();
    List<ContentsWithKey> expected = Arrays.asList(ContentsWithKey.of(a, ta), ContentsWithKey.of(b,  tb));
    assertThat(keys, Matchers.containsInAnyOrder(expected.toArray()));
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    tree.createReference(Branch.of(branch, null));
    Reference r = tree.getReferenceByName(branch);
    //ContentsKey k = ContentsKey.of("/%国","国.国");
    ContentsKey k = ContentsKey.of("a.b","c.d");
    IcebergTable ta = IcebergTable.of("path1");
    contents.setContents(k, branch, r.getHash(), "commit 1", ta);
    assertEquals(ContentsWithKey.of(k, ta), contents.getMultipleContents(branch, MultiGetContentsRequest.of(k)).getContents().get(0));
    assertEquals(ta, contents.getContents(k, branch));
    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkServerErrorPropagation() throws NessieNotFoundException, NessieConflictException {
    final String branch = "bar";
    tree.createReference(Branch.of(branch, null));
    NessieConflictException e = assertThrows(NessieConflictException.class, () -> tree.createReference(Branch.of(branch, null)));
    assertThat(e.getMessage(), Matchers.containsString("already exists"));
  }
}
