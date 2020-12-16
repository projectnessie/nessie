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

import static com.dremio.nessie.server.ReferenceMatchers.referenceWithNameAndType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.dremio.nessie.api.ContentsApi;
import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Branch;
import com.dremio.nessie.model.CommitMeta;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableMerge;
import com.dremio.nessie.model.ImmutableOperations;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.LogResponse;
import com.dremio.nessie.model.MultiGetContentsRequest;
import com.dremio.nessie.model.MultiGetContentsResponse.ContentsWithKey;
import com.dremio.nessie.model.Operations;
import com.dremio.nessie.model.Reference;
import com.dremio.nessie.model.Tag;
import com.dremio.nessie.versioned.VersionStore;
import com.dremio.nessie.versioned.memory.InMemoryVersionStore;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest {

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;

  @Inject
  VersionStore<Contents, CommitMeta> versionStore;

  @BeforeEach
  void init() throws Exception {
    String path = "http://localhost:19121/api/v1";
    this.client = new NessieClient(AuthType.NONE, path, null, null);
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    assertThat(versionStore, instanceOf(InMemoryVersionStore.class));
    ((InMemoryVersionStore<?, ?>)versionStore).clearUnsafe();
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "normal",
      "with space",
      "with%sign",
      "slash/thing",
      "all the/things%:*\u00E4\u00F6\u00FC" // some german umlauts as well
  })
  void referenceNames(String refNamePart) throws NessieNotFoundException, NessieConflictException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    String someHash = tree.getReferenceByName("main").getHash();

    tree.createReference(Tag.of(tagName, someHash));
    tree.createReference(Branch.of(branchName, someHash));
    tree.createReference(Branch.of(branchName2, someHash));

    Map<String, Reference> references = tree.getAllReferences().stream().collect(Collectors.toMap(Reference::getName, r -> r));
    assertThat(references.values(), containsInAnyOrder(
        referenceWithNameAndType("main", Branch.class),
        referenceWithNameAndType(tagName, Tag.class),
        referenceWithNameAndType(branchName, Branch.class),
        referenceWithNameAndType(branchName2, Branch.class)));

    Reference tagRef = references.get(tagName);
    Reference branchRef = references.get(branchName);
    Reference branchRef2 = references.get(branchName2);

    String tagHash = tagRef.getHash();
    String branchHash = branchRef.getHash();
    String branchHash2 = branchRef2.getHash();

    assertThat(tree.getReferenceByName(tagName), equalTo(tagRef));
    assertThat(tree.getReferenceByName(branchName), equalTo(branchRef));

    EntriesResponse entries = tree.getEntries(tagName);
    assertThat(entries, notNullValue());
    entries = tree.getEntries(branchName);
    assertThat(entries, notNullValue());

    LogResponse log = tree.getCommitLog(tagName);
    assertThat(log, notNullValue());
    log = tree.getCommitLog(branchName);
    assertThat(log, notNullValue());

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge, delete) will fail
    ImmutablePut op = ImmutablePut.builder().key(ContentsKey.of("some-key")).contents(IcebergTable.of("foo")).build();
    Operations ops = ImmutableOperations.builder().addOperations(op).build();
    tree.commitMultipleOperations(branchName, branchHash, "One dummy op", ops);
    log = tree.getCommitLog(branchName);
    String newHash = log.getOperations().get(0).getHash();

    tree.assignTag(tagName, tagHash, Tag.of(tagName, newHash));
    tree.assignBranch(branchName, newHash, Branch.of(branchName, newHash));

    tree.mergeRefIntoBranch(branchName2, branchHash2, ImmutableMerge.builder().fromHash(newHash).build());

    tree.deleteTag(tagName, newHash);
    tree.deleteBranch(branchName, newHash);
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
