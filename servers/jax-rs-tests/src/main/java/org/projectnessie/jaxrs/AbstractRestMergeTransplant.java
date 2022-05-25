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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.assertj.core.data.MapEntry.entry;

import com.google.common.collect.ImmutableList;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.BaseMergeTransplant.MergeBehavior;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.MergeResponse.ContentKeyDetails;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestMergeTransplant extends AbstractRestInvalidWithHttp {

  @ParameterizedTest
  @CsvSource(
      value = {"true,true", "true,false", "false,true", "false,false"}) // merge requires the hash
  public void transplant(boolean withDetachedCommit, boolean keepIndividualCommits)
      throws BaseNessieClientServerException {
    mergeTransplant(
        false,
        keepIndividualCommits,
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .transplantCommitsIntoBranch()
                .hashesToTransplant(ImmutableList.of(committed1.getHash(), committed2.getHash()))
                .fromRefName(maybeAsDetachedName(withDetachedCommit, source))
                .branch(target)
                .keepIndividualCommits(keepIndividualCommits)
                .returnConflictAsResult(returnConflictAsResult)
                .transplant());
  }

  @ParameterizedTest
  @CsvSource(
      value = {
        "UNCHANGED,true",
        "UNCHANGED,false",
        "DETACHED,true",
        "DETACHED,false"
      }) // merge requires the hash
  public void merge(ReferenceMode refMode, boolean keepIndividualCommits)
      throws BaseNessieClientServerException {
    mergeTransplant(
        !keepIndividualCommits,
        keepIndividualCommits,
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .mergeRefIntoBranch()
                .branch(target)
                .fromRef(refMode.transform(committed2))
                .keepIndividualCommits(keepIndividualCommits)
                .returnConflictAsResult(returnConflictAsResult)
                .merge());
  }

  @FunctionalInterface
  interface MergeTransplantActor {
    MergeResponse act(
        Branch target,
        Branch source,
        Branch committed1,
        Branch committed2,
        boolean returnConflictAsResult)
        throws NessieNotFoundException, NessieConflictException;
  }

  private void mergeTransplant(
      boolean verifyAdditionalParents, boolean keepIndividualCommits, MergeTransplantActor actor)
      throws BaseNessieClientServerException {
    Branch target = createBranch("base");
    Branch source = createBranch("branch");

    ContentKey key1 = ContentKey.of("key1");
    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    ContentKey key2 = ContentKey.of("key2");
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(source.getName())
            .hash(source.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch1"))
            .operation(Put.of(key1, table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    table1 =
        getApi()
            .getContent()
            .reference(committed1)
            .key(key1)
            .get()
            .get(key1)
            .unwrap(IcebergTable.class)
            .get();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(source.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch2"))
            .operation(Put.of(key1, table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    int commitCount = 2;

    LogResponse logBranch =
        getApi()
            .getCommitLog()
            .refName(source.getName())
            .untilHash(source.getHash())
            .maxRecords(commitCount)
            .get();

    Branch baseHead =
        getApi()
            .commitMultipleOperations()
            .branchName(target.getName())
            .hash(target.getHash())
            .commitMeta(CommitMeta.fromMessage("test-main"))
            .operation(Put.of(key2, table2))
            .commit();

    MergeResponse response = actor.act(target, source, committed1, committed2, false);
    Reference newHead =
        mergeWentFine(target, source, key1, key2, committed1, committed2, baseHead, response);

    // try again --> conflict

    assertThatThrownBy(() -> actor.act(target, source, committed1, committed2, false))
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessageContaining("keys have been changed in conflict");

    // try again --> conflict, but return information

    conflictExceptionReturnedAsMergeResult(
        actor, target, source, key1, key2, committed1, committed2, newHead);

    LogResponse log =
        getApi().getCommitLog().refName(target.getName()).untilHash(target.getHash()).get();
    if (keepIndividualCommits) {
      assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(3)
          .containsExactly("test-branch2", "test-branch1", "test-main");
    } else {
      assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(2)
          .first(InstanceOfAssertFactories.STRING)
          .contains("test-branch2")
          .contains("test-branch1");
    }

    // Verify that the commit-timestamp was updated
    LogResponse logOfMerged =
        getApi().getCommitLog().refName(target.getName()).maxRecords(commitCount).get();
    assertThat(
            logOfMerged.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime));

    assertThat(
            getApi().getEntries().refName(target.getName()).get().getEntries().stream()
                .map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("key1", "key2");

    if (verifyAdditionalParents) {
      assertThat(logOfMerged.getLogEntries())
          .first()
          .satisfies(
              logEntry ->
                  assertThat(logEntry)
                      .extracting(LogEntry::getAdditionalParents)
                      .asInstanceOf(list(String.class))
                      // When we can assume that all relevant Nessie clients are aware of the
                      // org.projectnessie.model.LogResponse.LogEntry.getAdditionalParents field,
                      // the following code can be uncommented. See also
                      // TreeApiImpl.commitToLogEntry().
                      // .hasSize(1)
                      // .first()
                      // .isEqualTo(committed2.getHash()),
                      .isEmpty(),
              logEntry ->
                  assertThat(logEntry)
                      .extracting(LogEntry::getCommitMeta)
                      .extracting(CommitMeta::getProperties)
                      .asInstanceOf(map(String.class, String.class))
                      .containsExactly(
                          entry(CommitMeta.MERGE_PARENT_PROPERTY, committed2.getHash())));
    }
  }

  private Reference mergeWentFine(
      Branch target,
      Branch source,
      ContentKey key1,
      ContentKey key2,
      Branch committed1,
      Branch committed2,
      Branch baseHead,
      MergeResponse response)
      throws NessieNotFoundException {
    Reference newHead = getApi().getReference().refName(target.getName()).get();
    assertThat(response)
        .satisfies(
            r ->
                assertThat(r)
                    .extracting(
                        MergeResponse::wasApplied,
                        MergeResponse::wasSuccessful,
                        MergeResponse::getExpectedHash,
                        MergeResponse::getTargetBranch,
                        MergeResponse::getEffectiveTargetHash,
                        MergeResponse::getResultantTargetHash)
                    .containsExactly(
                        true,
                        true,
                        source.getHash(),
                        target.getName(),
                        baseHead.getHash(),
                        newHead.getHash()),
            r ->
                assertThat(r)
                    .extracting(
                        MergeResponse::getCommonAncestor,
                        MergeResponse::getDetails,
                        MergeResponse::getSourceCommits,
                        MergeResponse::getTargetCommits)
                    .satisfiesExactly(
                        commonAncestor ->
                            assertThat(commonAncestor)
                                .satisfiesAnyOf(
                                    a -> assertThat(a).isNull(),
                                    b -> assertThat(b).isEqualTo(target.getHash())),
                        details ->
                            assertThat(details)
                                .asInstanceOf(list(ContentKeyDetails.class))
                                .extracting(
                                    ContentKeyDetails::getKey,
                                    ContentKeyDetails::getConflictType,
                                    ContentKeyDetails::getMergeBehavior)
                                .containsExactlyInAnyOrder(
                                    tuple(key1, ContentKeyConflict.NONE, MergeBehavior.NORMAL),
                                    tuple(key2, ContentKeyConflict.NONE, MergeBehavior.NORMAL)),
                        sourceCommits ->
                            assertThat(sourceCommits)
                                .asInstanceOf(list(LogEntry.class))
                                .extracting(LogEntry::getCommitMeta)
                                .extracting(CommitMeta::getHash, CommitMeta::getMessage)
                                .containsExactly(
                                    tuple(committed2.getHash(), "test-branch2"),
                                    tuple(committed1.getHash(), "test-branch1")),
                        targetCommits ->
                            assertThat(targetCommits)
                                .asInstanceOf(list(LogEntry.class))
                                .extracting(LogEntry::getCommitMeta)
                                .extracting(CommitMeta::getHash, CommitMeta::getMessage)
                                .containsExactly(tuple(baseHead.getHash(), "test-main"))));
    return newHead;
  }

  private static void conflictExceptionReturnedAsMergeResult(
      MergeTransplantActor actor,
      Branch target,
      Branch source,
      ContentKey key1,
      ContentKey key2,
      Branch committed1,
      Branch committed2,
      Reference newHead)
      throws NessieNotFoundException, NessieConflictException {
    MergeResponse conflictResult = actor.act(target, source, committed1, committed2, true);
    assertThat(conflictResult)
        .satisfies(
            r ->
                assertThat(r)
                    .extracting(
                        MergeResponse::wasApplied,
                        MergeResponse::wasSuccessful,
                        MergeResponse::getExpectedHash,
                        MergeResponse::getTargetBranch,
                        MergeResponse::getEffectiveTargetHash,
                        MergeResponse::getResultantTargetHash)
                    .containsExactly(
                        false,
                        false,
                        source.getHash(),
                        target.getName(),
                        newHead.getHash(),
                        newHead.getHash()),
            r ->
                assertThat(r)
                    .extracting(
                        MergeResponse::getCommonAncestor,
                        MergeResponse::getDetails,
                        MergeResponse::getSourceCommits,
                        MergeResponse::getTargetCommits)
                    .satisfiesExactly(
                        commonAncestor ->
                            assertThat(commonAncestor)
                                .satisfiesAnyOf(
                                    a -> assertThat(a).isNull(),
                                    b -> assertThat(b).isEqualTo(target.getHash())),
                        details ->
                            assertThat(details)
                                .asInstanceOf(list(ContentKeyDetails.class))
                                .extracting(
                                    ContentKeyDetails::getKey,
                                    ContentKeyDetails::getConflictType,
                                    ContentKeyDetails::getMergeBehavior)
                                .containsExactlyInAnyOrder(
                                    tuple(
                                        key1,
                                        ContentKeyConflict.UNRESOLVABLE,
                                        MergeBehavior.NORMAL),
                                    tuple(key2, ContentKeyConflict.NONE, MergeBehavior.NORMAL)),
                        sourceCommits ->
                            assertThat(sourceCommits)
                                .asInstanceOf(list(LogEntry.class))
                                .extracting(LogEntry::getCommitMeta)
                                .extracting(CommitMeta::getHash, CommitMeta::getMessage)
                                .containsExactly(
                                    tuple(committed2.getHash(), "test-branch2"),
                                    tuple(committed1.getHash(), "test-branch1")),
                        targetCommits ->
                            assertThat(targetCommits)
                                .asInstanceOf(list(LogEntry.class))
                                .extracting(LogEntry::getCommitMeta)
                                .extracting(CommitMeta::getMessage)
                                .containsAnyOf("test-branch2", "test-branch1", "test-main")));
  }

  @ParameterizedTest
  @EnumSource(
      value = ReferenceMode.class,
      mode = Mode.EXCLUDE,
      names = "NAME_ONLY") // merge requires the hash
  public void mergeWithNamespaces(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch base = createBranch("merge-base");
    Branch branch = createBranch("merge-branch");
    Namespace ns = Namespace.parse("a.b.c");
    // create the same namespace on both branches
    getApi().createNamespace().namespace(ns).refName(branch.getName()).create();
    getApi().createNamespace().namespace(ns).refName(base.getName()).create();

    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    ContentKey key1 = ContentKey.of(ns, "key1");
    ContentKey key2 = ContentKey.of(ns, "key2");
    Branch committed1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(branch.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch1"))
            .operation(Put.of(key1, table1))
            .commit();
    assertThat(committed1.getHash()).isNotNull();

    table1 =
        getApi()
            .getContent()
            .reference(committed1)
            .key(key1)
            .get()
            .get(key1)
            .unwrap(IcebergTable.class)
            .get();

    Branch committed2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch.getName())
            .hash(committed1.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch2"))
            .operation(Put.of(key1, table1, table1))
            .commit();
    assertThat(committed2.getHash()).isNotNull();

    getApi()
        .commitMultipleOperations()
        .branchName(base.getName())
        .hash(base.getHash())
        .commitMeta(CommitMeta.fromMessage("test-main"))
        .operation(Put.of(key2, table2))
        .commit();

    getApi()
        .mergeRefIntoBranch()
        .branch(base)
        .fromRef(refMode.transform(committed2))
        .keepIndividualCommits(true)
        .merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    assertThat(
            log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
        .containsExactly(
            "test-branch2",
            "test-branch1",
            "create namespace a.b.c",
            "test-main",
            "create namespace a.b.c");

    assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(Entry::getName))
        .containsExactlyInAnyOrder(key1, key2, ContentKey.of(ns.getElements()));

    assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get()).isNotNull();
  }
}
