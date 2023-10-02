/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.tools.compatibility.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.internal.NessieUpgradesExtension;
import org.projectnessie.versioned.storage.common.config.StoreConfig;

@ExtendWith(NessieUpgradesExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Tag("nessie-multi-env")
public class ITUpgradePath {

  @NessieVersion Version version;
  @NessieAPI NessieApiV1 api;

  @BeforeAll
  static void beforeAll() {
    // Do some "Nessie version just started" stuff in a @BeforeAll ...
  }

  @AfterAll
  static void afterAll() {
    // Do some "Nessie version stopping" stuff in a @BeforeAll ...
  }

  @BeforeEach
  void beforeEach() {
    // Usual before-each-test callback
  }

  @AfterEach
  void afterEach() {
    // Usual after-each-test callback
  }

  // //////////////////////////////////////////////////////////////////////////////////////////
  // Basic tests
  // //////////////////////////////////////////////////////////////////////////////////////////

  private static final String VERSION_BRANCH_PREFIX = "version-";
  private static Branch versionBranch;
  private static final Set<String> createdBranches = new HashSet<>();

  @Test
  @Order(101)
  void createReference() throws Exception {
    Branch main = api.getDefaultBranch();
    versionBranch = Branch.of(VERSION_BRANCH_PREFIX + version, main.getHash());
    createdBranches.add(versionBranch.getName());
    api.createReference().sourceRefName(main.getName()).reference(versionBranch).create();
  }

  @Order(102)
  @Test
  void getReferences() throws NessieNotFoundException {
    assertThat(
            api.getAllReferences().stream()
                .map(Reference::getName)
                .filter(ref -> ref.startsWith(VERSION_BRANCH_PREFIX)))
        .containsExactlyInAnyOrderElementsOf(createdBranches);
  }

  @Test
  @Order(103)
  void commit() throws Exception {
    ContentKey key = ContentKey.of("my-tables-table_name");
    IcebergTable content = IcebergTable.of("metadata-location-" + version, 42L, 43, 44, 45);
    String commitMessage = "hello world " + version;
    Put operation = Put.of(key, content);
    Branch branchNew =
        api.commitMultipleOperations()
            .commitMeta(CommitMeta.fromMessage(commitMessage))
            .operation(operation)
            .branch(versionBranch)
            .commit();
    assertThat(branchNew)
        .isNotEqualTo(versionBranch)
        .extracting(Branch::getName)
        .isEqualTo(versionBranch.getName());
  }

  @Test
  @Order(104)
  void commitLog() throws NessieNotFoundException {
    assertThat(
            api.getAllReferences().stream()
                .filter(r -> r.getName().startsWith(VERSION_BRANCH_PREFIX)))
        .isNotEmpty()
        .allSatisfy(
            ref -> {
              String versionFromRef = ref.getName().substring(VERSION_BRANCH_PREFIX.length());
              Stream<LogEntry> commitLog = api.getCommitLog().refName(ref.getName()).stream();
              String commitMessage = "hello world " + versionFromRef;
              assertThat(commitLog)
                  .hasSize(1)
                  .map(LogEntry::getCommitMeta)
                  .map(CommitMeta::getMessage)
                  .containsExactly(commitMessage);
            })
        .allSatisfy(
            ref -> {
              String versionFromRef = ref.getName().substring(VERSION_BRANCH_PREFIX.length());
              ContentKey key = ContentKey.of("my-tables-table_name");
              IcebergTable content =
                  IcebergTable.of("metadata-location-" + versionFromRef, 42L, 43, 44, 45);
              Map<ContentKey, Content> contents = api.getContent().reference(ref).key(key).get();
              assertThat(contents)
                  .hasSize(1)
                  .extractingByKey(key)
                  .extracting(AbstractCompatibilityTests::withoutContentId)
                  .isEqualTo(content);
            });
  }

  // //////////////////////////////////////////////////////////////////////////////////////////
  // Keys + values
  // //////////////////////////////////////////////////////////////////////////////////////////

  private static Branch keysUpgradeBranch;
  private static final Map<String, Map<ContentKey, IcebergTable>> keysUpgradeAtHash =
      new LinkedHashMap<>();
  private static int keysUpgradeSequence;
  private static final String EXPLICIT_KEYS_ELEMENT = "explicit-keys";
  // exceed both number-of-parents per commit-log-entry and per global-log-entry
  private static final int keysUpgradeCommitsPerVersion =
      Math.max(
              50, // was: NonTransactionalDatabaseAdapterConfig.DEFAULT_PARENTS_PER_GLOBAL_COMMIT,
              StoreConfig.DEFAULT_PARENTS_PER_COMMIT)
          + 15;

  // fields used for "many keys" (so many keys, that key-list-entities must be used)
  private static final String
      KEY_LONG_ELEMENT = // 400 chars, Guava's Strings.repeat breaks the test here :(
      "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
              + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
              + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
              + "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn";
  private static Map<ContentKey, IcebergTable> manyContentsWithLongKeys = Collections.emptyMap();
  private static final int KEYS_PER_COMMIT =
      StoreConfig.DEFAULT_MAX_INCREMENTAL_INDEX_SIZE / KEY_LONG_ELEMENT.length() * 2;

  @Test
  @Order(201)
  void keysUpgradeCreateBranch() throws Exception {
    assumeThat(keysUpgradeBranch).isNull();

    Branch main = api.getDefaultBranch();
    keysUpgradeBranch =
        (Branch)
            api.createReference()
                .sourceRefName(main.getName())
                .reference(Branch.of("keyUpgradeBranch", main.getHash()))
                .create();
  }

  @Test
  @Order(202)
  void keysUpgradeVerifyBefore() throws Exception {
    keysUpgradeVerify();
  }

  private void keysUpgradeVerify() throws NessieNotFoundException {
    // Verify the explicitly added/removed contents created by keysUpgradeAddCommits().
    // Do this for all written commits (~65 per version).
    // The number of keys equals the number of commits per version.
    List<ContentKey> keys =
        IntStream.range(0, keysUpgradeCommitsPerVersion)
            .mapToObj(i -> ContentKey.of(EXPLICIT_KEYS_ELEMENT + "-upgrade-table-" + i))
            .collect(Collectors.toList());
    for (Entry<String, Map<ContentKey, IcebergTable>> hashToKeyValues :
        keysUpgradeAtHash.entrySet()) {
      keysVerify(hashToKeyValues.getKey(), hashToKeyValues.getValue(), keys);
    }

    // Verify all (currently expected at HEAD) keys + contents written by
    // keysAddCommitWithManyKeysOnce() for the first exercised Nessie version, about 500-600 keys.
    if (!manyContentsWithLongKeys.isEmpty()) {
      keysUpgradeBranch = (Branch) api.getReference().refName(keysUpgradeBranch.getName()).get();
      keysVerify(
          keysUpgradeBranch.getHash(),
          manyContentsWithLongKeys,
          new ArrayList<>(manyContentsWithLongKeys.keySet()));
    }

    // Verify that "get keys" returns all per-version and all "long" content keys.
    Stream<ContentKey> expectedKeys = manyContentsWithLongKeys.keySet().stream();
    if (!keysUpgradeAtHash.isEmpty()) {
      // Very first call to 'keysUpgradeVerify' during the upgrade-path has no "relevant" commits
      // written by keysUpgradeAddCommits() yet, so only check 'keys', if keysUpgradeAtHash is not
      // empty (meaning: keysUpgradeAddCommits() did work).
      expectedKeys = Stream.concat(expectedKeys, keys.stream());
    }
    assertThat(
            api.getEntries().reference(keysUpgradeBranch).stream()
                .map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrderElementsOf(expectedKeys.collect(Collectors.toSet()));
  }

  private void keysVerify(
      String hash, Map<ContentKey, IcebergTable> expectedKeyValues, List<ContentKey> checkKeys)
      throws NessieNotFoundException {
    Map<ContentKey, Content> retrievedContents =
        api.getContent()
            .reference(Branch.of(keysUpgradeBranch.getName(), hash))
            .keys(checkKeys)
            .get();

    assertThat(expectedKeyValues)
        .describedAs(
            "Content objects for %d keys, version %s, commit %s", checkKeys.size(), version, hash)
        .allSatisfy(
            (key, table) ->
                assertThat(retrievedContents)
                    .extractingByKey(key, type(IcebergTable.class))
                    .extracting(IcebergTable::getSnapshotId, IcebergTable::getMetadataLocation)
                    .containsExactly(table.getSnapshotId(), table.getMetadataLocation()));
  }

  @Test
  @Order(203)
  void keysUpgradeAddCommits() throws Exception {
    Map<ContentKey, IcebergTable> currentKeyValues =
        keysUpgradeAtHash.getOrDefault(keysUpgradeBranch.getHash(), Collections.emptyMap());

    for (int i = 0; i < keysUpgradeCommitsPerVersion; i++) {
      ContentKey key = ContentKey.of(EXPLICIT_KEYS_ELEMENT + "-upgrade-table-" + i);
      if ((i % 10) == 9) {
        if (currentKeyValues.containsKey(key)) {
          keysUpgradeBranch =
              api.commitMultipleOperations()
                  .branch(keysUpgradeBranch)
                  .commitMeta(
                      CommitMeta.fromMessage(
                          "Commit #" + i + "/delete from Nessie version " + version))
                  .operation(Delete.of(key))
                  .commit();
          Map<ContentKey, IcebergTable> newKeyValues = new HashMap<>(currentKeyValues);
          newKeyValues.remove(key);
          keysUpgradeAtHash.put(keysUpgradeBranch.getHash(), newKeyValues);
        }
      }

      Content currentContent =
          api.getContent().refName(keysUpgradeBranch.getName()).key(key).get().get(key);
      IcebergTable newContent =
          currentContent == null
              ? IcebergTable.of(
                  "pointer-" + version + "-commit-" + i, keysUpgradeSequence++, i, i, i)
              : IcebergTable.of(
                  "pointer-" + version + "-commit-" + i,
                  keysUpgradeSequence++,
                  i,
                  i,
                  i,
                  currentContent.getId());
      Put put = Put.of(key, newContent);
      keysUpgradeBranch =
          api.commitMultipleOperations()
              .branch(keysUpgradeBranch)
              .commitMeta(
                  CommitMeta.fromMessage("Commit #" + i + "/put from Nessie version " + version))
              .operation(put)
              .commit();
      Map<ContentKey, IcebergTable> newKeyValues = new HashMap<>(currentKeyValues);
      newKeyValues.put(key, newContent);
      keysUpgradeAtHash.put(keysUpgradeBranch.getHash(), newKeyValues);
      currentKeyValues = newKeyValues;
    }
  }

  /**
   * Write a lot of keys * with long content-keys to force Nessie to write key-list-entities, added
   * for <a href="https://github.com/projectnessie/nessie/pull/4536">#4536</a>. Verified via {@link
   * #keysUpgradeVerify()}.
   */
  @Test
  @Order(204)
  void keysAddCommitWithManyKeysOnce() throws Exception {
    assumeThat(manyContentsWithLongKeys).isEmpty();

    CommitMultipleOperationsBuilder commitBuilder =
        api.commitMultipleOperations()
            .branch(keysUpgradeBranch)
            .commitMeta(
                CommitMeta.fromMessage("Commit w/ additional puts from Nessie version " + version));
    Map<ContentKey, IcebergTable> newKeyValues = new HashMap<>();

    for (int keyNum = 0; keyNum < KEYS_PER_COMMIT; keyNum++) {
      ContentKey additionalKey =
          ContentKey.of("v" + version.toString() + "-k" + keyNum + "-" + KEY_LONG_ELEMENT);
      IcebergTable table =
          IcebergTable.of(
              "additional-pointer-" + version + "-table-" + keyNum,
              keysUpgradeSequence++,
              42,
              43,
              44);
      commitBuilder.operation(Put.of(additionalKey, table));

      newKeyValues.put(additionalKey, table);
    }
    keysUpgradeBranch = commitBuilder.commit();
    manyContentsWithLongKeys = newKeyValues;
  }

  @Test
  @Order(205)
  void keysUpgradeVerifyAfter() throws Exception {
    keysUpgradeVerify();
  }
}
