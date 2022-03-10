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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
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
  void getReferences() {
    assertThat(
            api.getAllReferences().get().getReferences().stream()
                .map(Reference::getName)
                .filter(ref -> ref.startsWith(VERSION_BRANCH_PREFIX)))
        .containsExactlyInAnyOrderElementsOf(createdBranches);
  }

  @Test
  @Order(103)
  void commit() throws Exception {
    ContentKey key = ContentKey.of("my", "tables", "table_name");
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
  void commitLog() {
    assertThat(
            api.getAllReferences().get().getReferences().stream()
                .filter(r -> r.getName().startsWith(VERSION_BRANCH_PREFIX)))
        .isNotEmpty()
        .allSatisfy(
            ref -> {
              String versionFromRef = ref.getName().substring(VERSION_BRANCH_PREFIX.length());
              LogResponse commitLog = api.getCommitLog().refName(ref.getName()).get();
              String commitMessage = "hello world " + versionFromRef;
              assertThat(commitLog.getLogEntries())
                  .hasSize(1)
                  .map(LogEntry::getCommitMeta)
                  .map(CommitMeta::getMessage)
                  .containsExactly(commitMessage);
            })
        .allSatisfy(
            ref -> {
              String versionFromRef = ref.getName().substring(VERSION_BRANCH_PREFIX.length());
              ContentKey key = ContentKey.of("my", "tables", "table_name");
              IcebergTable content =
                  IcebergTable.of(
                      "metadata-location", 42L, 43, 44, 45, "content-id-" + versionFromRef);
              Map<ContentKey, Content> contents = api.getContent().reference(ref).key(key).get();
              assertThat(contents).containsExactly(entry(key, content));
            });
  }

  private void expectedRefLogEntry(String op) {
    if (version.compareTo(Version.parseVersion("0.18.0")) >= 0) {
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
    StreamingUtil.getReflogStream(api, b -> b, OptionalInt.empty()).forEach(logEntries::add);
    Collections.reverse(logEntries);
    assertThat(
            logEntries.stream()
                .filter(e -> !keysUpgradeBranch.getName().equals(e.getRefName()))
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
  // exceed both number-of-parents per commit-log-entry and per global-log-entry
  private static final int keysUpgradeCommitsPerVersion = 65;

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
    for (Entry<String, Map<ContentKey, IcebergTable>> hashToKeyValues :
        keysUpgradeAtHash.entrySet()) {
      Map<ContentKey, IcebergTable> expectedKeyValues = hashToKeyValues.getValue();

      Map<ContentKey, Content> retrievedContents =
          api.getContent()
              .reference(Branch.of(keysUpgradeBranch.getName(), hashToKeyValues.getKey()))
              .keys(
                  IntStream.range(0, keysUpgradeCommitsPerVersion)
                      .mapToObj(i -> ContentKey.of("keys.upgrade.table" + i))
                      .collect(Collectors.toList()))
              .get();

      assertThat(expectedKeyValues)
          .allSatisfy(
              (key, table) ->
                  assertThat(retrievedContents)
                      .extractingByKey(key, InstanceOfAssertFactories.type(IcebergTable.class))
                      .extracting(IcebergTable::getSnapshotId, IcebergTable::getId)
                      .containsExactly(table.getSnapshotId(), table.getId()));
    }
  }

  @Test
  @Order(203)
  void keysUpgradeAddCommits() throws Exception {
    keysUpgradeBranch = (Branch) api.getReference().refName(keysUpgradeBranch.getName()).get();

    Map<ContentKey, IcebergTable> currentKeyValues =
        keysUpgradeAtHash.getOrDefault(keysUpgradeBranch.getHash(), Collections.emptyMap());

    for (int i = 0; i < keysUpgradeCommitsPerVersion; i++) {
      ContentKey key = ContentKey.of("keys.upgrade.table" + i);
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
      newKeyValues.remove(key);
      keysUpgradeAtHash.put(keysUpgradeBranch.getHash(), newKeyValues);
    }
  }

  @Test
  @Order(204)
  void keysUpgradeVerifyAfter() throws Exception {
    keysUpgradeVerify();
  }
}
