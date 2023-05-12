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
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.StreamingUtil;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.GetCommitLogBuilder;
import org.projectnessie.client.api.GetEntriesBuilder;
import org.projectnessie.client.api.GetRefLogBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.RefLogResponse.RefLogResponseEntry;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.Version;
import org.projectnessie.tools.compatibility.api.VersionCondition;
import org.projectnessie.tools.compatibility.internal.NessieUpgradesExtension;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

@ExtendWith(NessieUpgradesExtension.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
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

  @SuppressWarnings("deprecation")
  Stream<Reference> allReferences() throws NessieNotFoundException {
    if (version.isGreaterThan(Version.parseVersion("0.30.0"))) {
      return api.getAllReferences().stream();
    } else {
      return StreamingUtil.getAllReferencesStream(api, Function.identity(), OptionalInt.empty());
    }
  }

  @SuppressWarnings("deprecation")
  Stream<EntriesResponse.Entry> entries(Function<GetEntriesBuilder, GetEntriesBuilder> configurer)
      throws NessieNotFoundException {
    if (version.isGreaterThan(Version.parseVersion("0.30.0"))) {
      return configurer.apply(api.getEntries()).stream();
    } else {
      return StreamingUtil.getEntriesStream(api, configurer, OptionalInt.empty());
    }
  }

  @SuppressWarnings("deprecation")
  Stream<LogEntry> commitLog(Function<GetCommitLogBuilder, GetCommitLogBuilder> configurer)
      throws NessieNotFoundException {
    if (version.isGreaterThan(Version.parseVersion("0.30.0"))) {
      return configurer.apply(api.getCommitLog()).stream();
    } else {
      return StreamingUtil.getCommitLogStream(api, configurer, OptionalInt.empty());
    }
  }

  @SuppressWarnings("deprecation")
  Stream<RefLogResponseEntry> refLog(Function<GetRefLogBuilder, GetRefLogBuilder> configurer)
      throws NessieNotFoundException {
    if (version.isGreaterThan(Version.parseVersion("0.30.0"))) {
      return configurer.apply(api.getRefLog()).stream();
    } else {
      return StreamingUtil.getReflogStream(api, configurer, OptionalInt.empty());
    }
  }

  // //////////////////////////////////////////////////////////////////////////////////////////
  // Basic tests
  // //////////////////////////////////////////////////////////////////////////////////////////

  private static final String VERSION_BRANCH_PREFIX = "version-";
  private static Branch versionBranch;
  private static final Set<String> createdBranches = new HashSet<>();
  private static final Map<String, List<String>> expectedRefLog = new LinkedHashMap<>();

  @Test
  @Order(101)
  void createReference() throws Exception {
    Branch main = api.getDefaultBranch();
    versionBranch = Branch.of(VERSION_BRANCH_PREFIX + version, main.getHash());
    createdBranches.add(versionBranch.getName());
    api.createReference().sourceRefName(main.getName()).reference(versionBranch).create();

    expectedRefLogEntry("CREATE_REFERENCE");
  }

  @Order(102)
  @Test
  void getReferences() throws NessieNotFoundException {
    assertThat(
            allReferences()
                .map(Reference::getName)
                .filter(ref -> ref.startsWith(VERSION_BRANCH_PREFIX)))
        .containsExactlyInAnyOrderElementsOf(createdBranches);
  }

  @Test
  @Order(103)
  void commit() throws Exception {
    ContentKey key = ContentKey.of("my-tables-table_name");
    IcebergTable content =
        IcebergTable.of("metadata-location", 42L, 43, 44, 45, "content-id-" + version);
    String commitMessage = "hello world " + version;
    Put operation = Put.of(key, content);
    Branch branchNew =
        commitMaybeRetry(
            api.commitMultipleOperations()
                .commitMeta(CommitMeta.fromMessage(commitMessage))
                .operation(operation)
                .branch(versionBranch));
    assertThat(branchNew)
        .isNotEqualTo(versionBranch)
        .extracting(Branch::getName)
        .isEqualTo(versionBranch.getName());

    expectedRefLogEntry("COMMIT");
  }

  // Workaround for broken key-collision checks in RocksDB
  private Branch commitMaybeRetry(CommitMultipleOperationsBuilder commitBuilder)
      throws NessieNotFoundException, NessieConflictException {
    // WORKAROUND for https://github.com/projectnessie/nessie/pull/3413
    while (true) {
      try {
        return commitBuilder.commit();
      } catch (NessieReferenceConflictException e) {
        if (!"Hash collision detected".equals(e.getMessage())
            || Version.parseVersion("0.20.1").isLessThan(version)) {
          throw e;
        }
      }
    }
  }

  @Test
  @Order(104)
  void commitLog() throws NessieNotFoundException {
    assertThat(allReferences().filter(r -> r.getName().startsWith(VERSION_BRANCH_PREFIX)))
        .isNotEmpty()
        .allSatisfy(
            ref -> {
              String versionFromRef = ref.getName().substring(VERSION_BRANCH_PREFIX.length());
              Stream<LogEntry> commitLog = commitLog(b -> b.refName(ref.getName()));
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
                  IcebergTable.of(
                      "metadata-location", 42L, 43, 44, 45, "content-id-" + versionFromRef);
              Map<ContentKey, Content> contents = api.getContent().reference(ref).key(key).get();
              assertThat(contents).containsExactly(entry(key, content));
            });
  }

  // //////////////////////////////////////////////////////////////////////////////////////////
  // ref log
  // //////////////////////////////////////////////////////////////////////////////////////////

  private void expectedRefLogEntry(String op) {
    if (version.isGreaterThanOrEqual(Version.parseVersion("0.18.0"))) {
      if (version.isGreaterThanOrEqual(Version.REFLOG_FOR_COMMIT_REMOVED)) {
        switch (op) {
          case "CREATE_REFERENCE":
          case "DROP_REFERENCE":
          case "ASSIGN_REFERENCE":
            break;
          case "COMMIT":
          case "MERGE":
          case "TRANSPLANT":
          default:
            return;
        }
      }
      expectedRefLog.computeIfAbsent(versionBranch.getName(), x -> new ArrayList<>()).add(op);
    }
  }

  @Test
  @Order(105)
  @VersionCondition(minVersion = "0.18.0")
  void refLog() throws Exception {
    List<Tuple> allExpected =
        expectedRefLog.entrySet().stream()
            .flatMap(e -> e.getValue().stream().map(op -> tuple(e.getKey(), op)))
            .collect(Collectors.toList());

    ArrayList<RefLogResponseEntry> logEntries = new ArrayList<>();
    refLog(Function.identity()).forEach(logEntries::add);
    Collections.reverse(logEntries);
    assertThat(
            logEntries.stream()
                // When upgrade path starts >= 0.18.0, there'll be a reflog entry for this
                .filter(e -> !e.getRefName().equals("main"))
                // When upgrade path starts >= 0.18.0, this test will be executed before
                // keysUpgradeBranch is set
                .filter(
                    e ->
                        keysUpgradeBranch == null
                            || !keysUpgradeBranch.getName().equals(e.getRefName()))
                .map(e -> tuple(e.getRefName(), e.getOperation())))
        .containsExactlyElementsOf(allExpected);
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
              DatabaseAdapterConfig.DEFAULT_PARENTS_PER_COMMIT)
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
      DatabaseAdapterConfig.DEFAULT_MAX_ENTITY_SIZE / KEY_LONG_ELEMENT.length() * 2;

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
    assertThat(entries(b -> b.reference(keysUpgradeBranch)).map(EntriesResponse.Entry::getName))
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
                    .extractingByKey(key, InstanceOfAssertFactories.type(IcebergTable.class))
                    .extracting(IcebergTable::getSnapshotId, IcebergTable::getId)
                    .containsExactly(table.getSnapshotId(), table.getId()));
  }

  @Test
  @Order(203)
  void keysUpgradeAddCommits() throws Exception {
    Map<ContentKey, IcebergTable> currentKeyValues =
        keysUpgradeAtHash.getOrDefault(keysUpgradeBranch.getHash(), Collections.emptyMap());

    for (int i = 0; i < keysUpgradeCommitsPerVersion; i++) {
      ContentKey key = ContentKey.of(EXPLICIT_KEYS_ELEMENT + "-upgrade-table-" + i);
      if ((i % 10) == 9) {
        keysUpgradeBranch =
            commitMaybeRetry(
                api.commitMultipleOperations()
                    .branch(keysUpgradeBranch)
                    .commitMeta(
                        CommitMeta.fromMessage(
                            "Commit #" + i + "/delete from Nessie version " + version))
                    .operation(Delete.of(key)));
        Map<ContentKey, IcebergTable> newKeyValues = new HashMap<>(currentKeyValues);
        newKeyValues.remove(key);
        keysUpgradeAtHash.put(keysUpgradeBranch.getHash(), newKeyValues);
      }

      Content currentContent =
          api.getContent().refName(keysUpgradeBranch.getName()).key(key).get().get(key);
      String cid = currentContent == null ? "table-" + i + "-" + version : currentContent.getId();
      IcebergTable newContent =
          IcebergTable.of(
              "pointer-" + version + "-commit-" + i, keysUpgradeSequence++, i, i, i, cid);
      Put put =
          currentContent != null
              ? Put.of(key, newContent, currentContent)
              : Put.of(key, newContent);
      keysUpgradeBranch =
          commitMaybeRetry(
              api.commitMultipleOperations()
                  .branch(keysUpgradeBranch)
                  .commitMeta(
                      CommitMeta.fromMessage(
                          "Commit #" + i + "/put from Nessie version " + version))
                  .operation(put));
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
          ContentKey.of("v" + version.toString(), "k" + keyNum, KEY_LONG_ELEMENT);
      String additionalCid = "additional-table-" + keyNum + "-" + "-" + version;
      IcebergTable table =
          IcebergTable.of(
              "additional-pointer-" + version + "-table-" + keyNum,
              keysUpgradeSequence++,
              42,
              43,
              44,
              additionalCid);
      commitBuilder.operation(Put.of(additionalKey, table));

      newKeyValues.put(additionalKey, table);
    }
    keysUpgradeBranch = commitMaybeRetry(commitBuilder);
    manyContentsWithLongKeys = newKeyValues;
  }

  @Test
  @Order(205)
  void keysUpgradeVerifyAfter() throws Exception {
    keysUpgradeVerify();
  }
}
