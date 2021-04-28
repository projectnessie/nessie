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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.model.Validation.HASH_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_MESSAGE;
import static org.projectnessie.model.Validation.REF_NAME_OR_HASH_MESSAGE;
import static org.projectnessie.server.ReferenceMatchers.referenceWithNameAndType;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.ContentsApi;
import org.projectnessie.api.TreeApi;
import org.projectnessie.client.NessieClient;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.http.HttpClient;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.rest.NessieBadRequestException;
import org.projectnessie.client.rest.NessieHttpResponseFilter;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Contents;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.Hash;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableMerge;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;
import org.projectnessie.model.ImmutableSqlView;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.MultiGetContentsRequest;
import org.projectnessie.model.MultiGetContentsResponse.ContentsWithKey;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Operations;
import org.projectnessie.model.Reference;
import org.projectnessie.model.SqlView;
import org.projectnessie.model.Tag;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.ImmutableList;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
class TestRest {

  public static final String COMMA_VALID_HASH_1 = ",1234567890123456789012345678901234567890123456789012345678901234";
  public static final String COMMA_VALID_HASH_2 = ",1234567890123456789012345678901234567890";
  public static final String COMMA_VALID_HASH_3 = ",1234567890123456";

  private NessieClient client;
  private TreeApi tree;
  private ContentsApi contents;
  private HttpClient httpClient;

  @BeforeEach
  void init() {
    URI uri = URI.create("http://localhost:19121/api/v1");
    client = NessieClient.builder().withUri(uri).build();
    tree = client.getTreeApi();
    contents = client.getContentsApi();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT)
        .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    httpClient = HttpClient.builder().setBaseUri(uri).setObjectMapper(mapper).build();
    httpClient.register(new NessieHttpResponseFilter(mapper));
  }

  @AfterEach
  void closeClient() {
    client.close();
  }

  @Test
  void createReferences() throws NessieNotFoundException {
    String mainHash = tree.getReferenceByName("main").getHash();

    String tagName1 = "createReferences_tag1";
    String tagName2 = "createReferences_tag2";
    String branchName1 = "createReferences_branch1";
    String branchName2 = "createReferences_branch2";
    assertAll(
        // Tag without hash
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.createReference(Tag.of(tagName1, null))).getMessage(),
            startsWith("Bad Request (HTTP/400): Cannot create an unassigned tag reference")),
        // legit Tag with name + hash
        () -> {
          Reference refTag1 = tree.createReference(Tag.of(tagName2, mainHash));
          assertEquals(Tag.of(tagName2, mainHash), refTag1);
        },
        // Branch without hash
        () -> {
          Reference refBranch1 = tree.createReference(Branch.of(branchName1, null));
          assertEquals(Branch.of(branchName1, mainHash), refBranch1);
        },
        // Branch with name + hash
        () -> {
          Reference refBranch2 = tree.createReference(Branch.of(branchName2, mainHash));
          assertEquals(Branch.of(branchName2, mainHash), refBranch2);
        },
        // Hash
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.createReference(Hash.of("cafebabedeafbeef"))).getMessage(),
            startsWith("Bad Request (HTTP/400): Only tag and branch references can be created")));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "normal",
      "with-no_space",
      "slash/thing"
  })
  void referenceNames(String refNamePart) throws NessieNotFoundException, NessieConflictException {
    String tagName = "tag" + refNamePart;
    String branchName = "branch" + refNamePart;
    String branchName2 = "branch2" + refNamePart;

    String someHash = tree.getReferenceByName("main").getHash();

    Reference createdTag = tree.createReference(Tag.of(tagName, someHash));
    assertEquals(Tag.of(tagName, someHash), createdTag);
    Reference creadedBranch1 = tree.createReference(Branch.of(branchName, someHash));
    assertEquals(Branch.of(branchName, someHash), creadedBranch1);
    Reference creadedBranch2 = tree.createReference(Branch.of(branchName2, someHash));
    assertEquals(Branch.of(branchName2, someHash), creadedBranch2);

    Map<String, Reference> references = tree.getAllReferences().stream()
        .filter(r -> "main".equals(r.getName()) || r.getName().endsWith(refNamePart))
        .collect(Collectors.toMap(Reference::getName, Function.identity()));
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

    EntriesResponse entries = tree.getEntries(tagName, null, null, Collections.emptyList());
    assertThat(entries, notNullValue());
    entries = tree.getEntries(branchName, null, null, Collections.emptyList());
    assertThat(entries, notNullValue());

    LogResponse log = tree.getCommitLog(tagName, null, null);
    assertThat(log, notNullValue());
    log = tree.getCommitLog(branchName, null, null);
    assertThat(log, notNullValue());

    // Need to have at least one op, otherwise all following operations (assignTag/Branch, merge, delete) will fail
    ImmutablePut op = ImmutablePut.builder().key(ContentsKey.of("some-key")).contents(IcebergTable.of("foo")).build();
    Operations ops = ImmutableOperations.builder().addOperations(op).commitMeta(CommitMeta.fromMessage("One dummy op")).build();
    tree.commitMultipleOperations(branchName, branchHash, ops);
    log = tree.getCommitLog(branchName, null, null);
    String newHash = log.getOperations().get(0).getHash();

    tree.assignTag(tagName, tagHash, Tag.of(tagName, newHash));
    tree.assignBranch(branchName, newHash, Branch.of(branchName, newHash));

    tree.mergeRefIntoBranch(branchName2, branchHash2, ImmutableMerge.builder().fromHash(newHash).build());

    tree.deleteTag(tagName, newHash);
    tree.deleteBranch(branchName, newHash);
  }

  @Test
  void commitLogPaging() throws NessieNotFoundException, NessieConflictException {
    String someHash = tree.getReferenceByName("main").getHash();
    String branchName = "commitLogPaging";
    Branch branch = Branch.of(branchName, someHash);
    tree.createReference(branch);

    int commits = 95;
    int pageSizeHint = 10;

    String currentHash = someHash;
    List<String> allMessages = new ArrayList<>();
    for (int i = 0; i < commits; i++) {
      String msg = "message-for-" + i;
      allMessages.add(msg);
      String nextHash = tree.commitMultipleOperations(branchName, currentHash,
          ImmutableOperations.builder()
              .commitMeta(CommitMeta.fromMessage(msg))
              .addOperations(Put.of(ContentsKey.of("table"), IcebergTable.of("some-file-" + i)))
              .build()).getHash();
      assertNotEquals(currentHash, nextHash);
      currentHash = nextHash;
    }
    Collections.reverse(allMessages);

    String pageToken = null;
    for (int pos = 0; pos < commits; pos += pageSizeHint) {
      LogResponse response = tree.getCommitLog(branchName, pageSizeHint, pageToken);
      if (pos + pageSizeHint <= commits) {
        assertTrue(response.hasMore());
        assertNotNull(response.getToken());
        assertEquals(
            allMessages.subList(pos, pos + pageSizeHint),
            response.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList())
        );
        pageToken = response.getToken();
      } else {
        assertFalse(response.hasMore());
        assertNull(response.getToken());
        assertEquals(
            allMessages.subList(pos, allMessages.size()),
            response.getOperations().stream().map(CommitMeta::getMessage).collect(Collectors.toList())
        );
        break;
      }
    }

    List<CommitMeta> completeLog = StreamingUtil.getCommitLogStream(tree, branchName, OptionalInt.of(pageSizeHint))
        .collect(Collectors.toList());
    assertEquals(
        completeLog.stream().map(CommitMeta::getMessage).collect(Collectors.toList()),
        allMessages
    );
  }

  @Test
  void multiget() throws NessieNotFoundException, NessieConflictException {
    final String branch = "foo";
    Reference r = tree.createReference(Branch.of(branch, null));
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
  void filters() throws NessieNotFoundException, NessieConflictException {
    final String branch = "filterTypes";
    Reference r = tree.createReference(Branch.of(branch, null));
    ContentsKey a = ContentsKey.of("a");
    ContentsKey b = ContentsKey.of("b");
    IcebergTable ta = IcebergTable.of("path1");
    SqlView tb = ImmutableSqlView.builder().sqlText("select * from table")
        .dialect(SqlView.Dialect.DREMIO).build();
    contents.setContents(a, branch, r.getHash(), "commit 1", ta);
    contents.setContents(b, branch, r.getHash(), "commit 2", tb);
    List<EntriesResponse.Entry> entries = tree.getEntries(branch, null, null, Collections.emptyList()).getEntries();
    List<EntriesResponse.Entry> expected = Arrays.asList(
        EntriesResponse.Entry.builder().name(a).type(Contents.Type.ICEBERG_TABLE).build(),
        EntriesResponse.Entry.builder().name(b).type(Contents.Type.VIEW).build());
    assertThat(entries, Matchers.containsInAnyOrder(expected.toArray()));
    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.ICEBERG_TABLE.name())).getEntries();
    assertEquals(Collections.singletonList(expected.get(0)), entries);

    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.VIEW.name())).getEntries();
    assertEquals(Collections.singletonList(expected.get(1)), entries);

    entries = tree.getEntries(branch, null, null, ImmutableList.of(Contents.Type.VIEW.name(),
        Contents.Type.ICEBERG_TABLE.name())).getEntries();
    assertThat(entries, Matchers.containsInAnyOrder(expected.toArray()));

    tree.deleteBranch(branch, tree.getReferenceByName(branch).getHash());
  }

  @Test
  void checkSpecialCharacterRoundTrip() throws NessieNotFoundException, NessieConflictException {
    final String branch = "specialchar";
    Reference r = tree.createReference(Branch.of(branch, null));
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

  @ParameterizedTest
  @CsvSource({
      "x/" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidBranchNames(String invalidBranchName, String validHash) {
    Operations ops = ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("")).build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(invalidBranchName, validHash, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(invalidBranchName, validHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getCommitLog.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getCommitLog(invalidBranchName, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getEntries.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getEntries(invalidBranchName, null, null, Collections.emptyList())).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getReferenceByName.refName: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.getReferenceByName(invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(invalidBranchName, validHash, tag)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.mergeRefIntoBranch(invalidBranchName, validHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("mergeRefIntoBranch.branchName: " + REF_NAME_MESSAGE),
                containsString("mergeRefIntoBranch.merge: must not be null")
            )),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.tagName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(invalidBranchName, validHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.branchName: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(invalidBranchName, validHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, invalidBranchName, validHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.branch: " + REF_NAME_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, invalidBranchName, validHash, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getContents(key, invalidBranchName)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.getMultipleContents(invalidBranchName, mgReq)).getMessage())
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidHashes(String invalidHashIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidHash = invalidHashIn != null ? invalidHashIn : "";

    String validBranchName = "hello";
    Operations ops = ImmutableOperations.builder().commitMeta(CommitMeta.fromMessage("")).build();
    ContentsKey key = ContentsKey.of("x");
    Contents cts = IcebergTable.of("moo");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    Tag tag = Tag.of("valid", validHash);
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): commitMultipleOperations.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.commitMultipleOperations(validBranchName, invalidHash, ops)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteBranch(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): assignTag.oldHash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.assignTag(validBranchName, invalidHash, tag)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> tree.mergeRefIntoBranch(validBranchName, invalidHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("mergeRefIntoBranch.merge: must not be null"),
                containsString("mergeRefIntoBranch.hash: " + HASH_MESSAGE)
            )),
        () -> assertEquals("Bad Request (HTTP/400): deleteTag.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.deleteTag(validBranchName, invalidHash)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): transplantCommitsIntoBranch.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> tree.transplantCommitsIntoBranch(validBranchName, invalidHash, null, null)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): setContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.setContents(key, validBranchName, invalidHash, null, cts)).getMessage()),
        () -> assertEquals("Bad Request (HTTP/400): deleteContents.hash: " + HASH_MESSAGE,
            assertThrows(NessieBadRequestException.class,
                () -> contents.deleteContents(key, validBranchName, invalidHash, null)).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> contents.getMultipleContents(invalidHash, null)).getMessage(),
            allOf(
                containsString("Bad Request (HTTP/400): "),
                containsString("getMultipleContents.request: must not be null"),
                containsString("getMultipleContents.ref: " + REF_NAME_OR_HASH_MESSAGE)
            ))
    );
  }

  @ParameterizedTest
  @CsvSource({
      "" + COMMA_VALID_HASH_1,
      "abc'" + COMMA_VALID_HASH_1,
      ".foo" + COMMA_VALID_HASH_2,
      "abc'def'..'blah" + COMMA_VALID_HASH_2,
      "abc'de..blah" + COMMA_VALID_HASH_3,
      "abc'de@{blah" + COMMA_VALID_HASH_3
  })
  void invalidTags(String invalidTagNameIn, String validHash) {
    // CsvSource maps an empty string as null
    String invalidTagName = invalidTagNameIn != null ? invalidTagNameIn : "";

    String validBranchName = "hello";
    ContentsKey key = ContentsKey.of("x");
    MultiGetContentsRequest mgReq = MultiGetContentsRequest.of(key);
    // Need the string-ified JSON representation of `Tag` here, because `Tag` itself performs
    // validation.
    String tag = "{\"type\": \"TAG\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String branch = "{\"type\": \"BRANCH\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    String different = "{\"type\": \"FOOBAR\", \"name\": \"" + invalidTagName + "\", \"hash\": \"" + validHash + "\"}";
    assertAll(
        () -> assertEquals("Bad Request (HTTP/400): assignTag.tag: must not be null",
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(null))
            ).getMessage()),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(tag))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Cannot construct instance of "
                + "`org.projectnessie.model.ImmutableTag`, problem: "
                + REF_NAME_MESSAGE + " - but was: " + invalidTagName + "\n")),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(branch))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Could not resolve type id 'BRANCH' as a subtype of "
                + "`org.projectnessie.model.Tag`: Class `org.projectnessie.model.Branch` "
                + "not subtype of `org.projectnessie.model.Tag`\n")),
        () -> assertThat(
            assertThrows(NessieBadRequestException.class,
                () -> unwrap(() ->
                    httpClient.newRequest().path("trees/tag/{tagName}")
                        .resolveTemplate("tagName", validBranchName)
                        .queryParam("expectedHash", validHash)
                        .put(different))
            ).getMessage(),
            startsWith("Bad Request (HTTP/400): Could not resolve type id 'FOOBAR' as a subtype of "
                + "`org.projectnessie.model.Tag`: known type ids = []\n"))
    );
  }

  void unwrap(Executable exec) throws Throwable {
    try {
      exec.execute();
    } catch (Throwable targetException) {
      if (targetException instanceof HttpClientException) {
        if (targetException.getCause() instanceof NessieNotFoundException
            || targetException.getCause() instanceof NessieConflictException) {
          throw targetException.getCause();
        }
      }

      throw targetException;
    }
  }
}
