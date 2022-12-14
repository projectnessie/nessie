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
package org.projectnessie.jaxrs.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.assertj.core.data.MapEntry.entry;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.MergeReferenceBuilder;
import org.projectnessie.client.api.MergeTransplantBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.api.TransplantCommitsBuilder;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieApiVersions;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse.Entry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.MergeResponse.ContentKeyDetails;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestMergeTransplant extends AbstractRestInvalid {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void transplantKeepCommits(boolean withDetachedCommit)
      throws BaseNessieClientServerException {
    testTransplant(withDetachedCommit, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  @NessieApiVersions(versions = NessieApiVersion.V1) // API V2 does not allow squashed transplants
  public void transplantSquashed(boolean withDetachedCommit)
      throws BaseNessieClientServerException {
    testTransplant(withDetachedCommit, false);
  }

  private void testTransplant(boolean withDetachedCommit, boolean keepIndividualCommits)
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
  @EnumSource(names = {"UNCHANGED", "DETACHED"}) // hash is required
  @NessieApiVersions(versions = NessieApiVersion.V1) // API V2 does not allow unsquashed merges
  public void mergeKeepCommits(ReferenceMode refMode) throws BaseNessieClientServerException {
    testMerge(refMode, true);
  }

  @ParameterizedTest
  @EnumSource(names = {"UNCHANGED", "DETACHED"}) // hash is required
  public void mergeSquashed(ReferenceMode refMode) throws BaseNessieClientServerException {
    testMerge(refMode, false);
  }

  private void testMerge(ReferenceMode refMode, boolean keepIndividualCommits)
      throws BaseNessieClientServerException {
    // API v2 always squashed merges
    if (getApi() instanceof NessieApiV2) {
      Assumptions.assumeFalse(keepIndividualCommits);
    }

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
    Branch root = createBranch("root");

    root =
        getApi()
            .commitMultipleOperations()
            .branch(root)
            .commitMeta(CommitMeta.fromMessage("root"))
            .operation(Put.of(ContentKey.of("other"), IcebergTable.of("/dev/null", 42, 42, 42, 42)))
            .commit();

    Branch target = createBranch("base", root);
    Branch source = createBranch("branch", root);

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
    soft.assertThat(committed1.getHash()).isNotNull();

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
    soft.assertThat(committed2.getHash()).isNotNull();

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
        mergeWentFine(target, source, key1, committed1, committed2, baseHead, response);

    // try again --> conflict

    soft.assertThatThrownBy(() -> actor.act(target, source, committed1, committed2, false))
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessageContaining("keys have been changed in conflict");

    // try again --> conflict, but return information

    conflictExceptionReturnedAsMergeResult(
        actor, target, source, key1, committed1, committed2, newHead);

    LogResponse log =
        getApi().getCommitLog().refName(target.getName()).untilHash(target.getHash()).get();
    if (keepIndividualCommits) {
      soft.assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .containsExactly("test-branch2", "test-branch1", "test-main", "root");
    } else {
      soft.assertThat(
              log.getLogEntries().stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(3)
          .first(InstanceOfAssertFactories.STRING)
          .contains("test-branch2")
          .contains("test-branch1");
    }

    // Verify that the commit-timestamp was updated
    LogResponse logOfMerged =
        getApi().getCommitLog().refName(target.getName()).maxRecords(commitCount).get();
    soft.assertThat(
            logOfMerged.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getCommitTime));

    soft.assertThat(
            getApi().getEntries().refName(target.getName()).get().getEntries().stream()
                .map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("other", "key1", "key2");

    if (verifyAdditionalParents) {
      soft.assertThat(logOfMerged.getLogEntries())
          .first()
          .extracting(LogEntry::getAdditionalParents)
          .asInstanceOf(list(String.class))
          // When we can assume that all relevant Nessie clients are aware of the
          // org.projectnessie.model.LogResponse.LogEntry.getAdditionalParents field,
          // the following code can be uncommented. See also
          // TreeApiImpl.commitToLogEntry().
          // .hasSize(1)
          // .first()
          // .isEqualTo(committed2.getHash()),
          .isEmpty();
      soft.assertThat(logOfMerged.getLogEntries())
          .first()
          .extracting(LogEntry::getCommitMeta)
          .extracting(CommitMeta::getProperties)
          .asInstanceOf(map(String.class, String.class))
          .containsExactly(entry(CommitMeta.MERGE_PARENT_PROPERTY, committed2.getHash()));
    }
  }

  private Reference mergeWentFine(
      Branch target,
      Branch source,
      ContentKey key1,
      Branch committed1,
      Branch committed2,
      Branch baseHead,
      MergeResponse response)
      throws NessieNotFoundException {
    Reference newHead = getApi().getReference().refName(target.getName()).get();
    soft.assertThat(response)
        .extracting(
            MergeResponse::wasApplied,
            MergeResponse::wasSuccessful,
            MergeResponse::getExpectedHash,
            MergeResponse::getTargetBranch,
            MergeResponse::getEffectiveTargetHash,
            MergeResponse::getResultantTargetHash)
        .containsExactly(
            true, true, source.getHash(), target.getName(), baseHead.getHash(), newHead.getHash());

    soft.assertThat(response.getCommonAncestor())
        .satisfiesAnyOf(
            a -> assertThat(a).isNull(), b -> assertThat(b).isEqualTo(target.getHash()));
    soft.assertThat(response.getDetails())
        .asInstanceOf(list(ContentKeyDetails.class))
        .extracting(
            ContentKeyDetails::getKey,
            ContentKeyDetails::getConflictType,
            ContentKeyDetails::getMergeBehavior)
        .contains(tuple(key1, ContentKeyConflict.NONE, MergeBehavior.NORMAL));
    soft.assertThat(response.getSourceCommits())
        .asInstanceOf(list(LogEntry.class))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getHash, CommitMeta::getMessage)
        .containsExactly(
            tuple(committed2.getHash(), "test-branch2"),
            tuple(committed1.getHash(), "test-branch1"));
    soft.assertThat(response.getTargetCommits())
        .asInstanceOf(list(LogEntry.class))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getHash, CommitMeta::getMessage)
        .contains(tuple(baseHead.getHash(), "test-main"));

    return newHead;
  }

  private void conflictExceptionReturnedAsMergeResult(
      MergeTransplantActor actor,
      Branch target,
      Branch source,
      ContentKey key1,
      Branch committed1,
      Branch committed2,
      Reference newHead)
      throws NessieNotFoundException, NessieConflictException {
    MergeResponse conflictResult = actor.act(target, source, committed1, committed2, true);

    soft.assertThat(conflictResult)
        .extracting(
            MergeResponse::wasApplied,
            MergeResponse::wasSuccessful,
            MergeResponse::getExpectedHash,
            MergeResponse::getTargetBranch,
            MergeResponse::getEffectiveTargetHash)
        .containsExactly(false, false, source.getHash(), target.getName(), newHead.getHash());

    soft.assertThat(conflictResult.getCommonAncestor())
        .satisfiesAnyOf(
            a -> assertThat(a).isNull(), b -> assertThat(b).isEqualTo(target.getHash()));
    soft.assertThat(conflictResult.getDetails())
        .asInstanceOf(list(ContentKeyDetails.class))
        .extracting(
            ContentKeyDetails::getKey,
            ContentKeyDetails::getConflictType,
            ContentKeyDetails::getMergeBehavior)
        .contains(tuple(key1, ContentKeyConflict.UNRESOLVABLE, MergeBehavior.NORMAL));
    soft.assertThat(conflictResult.getSourceCommits())
        .asInstanceOf(list(LogEntry.class))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getHash, CommitMeta::getMessage)
        .containsExactly(
            tuple(committed2.getHash(), "test-branch2"),
            tuple(committed1.getHash(), "test-branch1"));
    soft.assertThat(conflictResult.getTargetCommits())
        .asInstanceOf(list(LogEntry.class))
        .extracting(LogEntry::getCommitMeta)
        .extracting(CommitMeta::getMessage)
        .containsAnyOf("test-branch2", "test-branch1", "test-main");
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V2) // Merge message is not settable in V1
  public void mergeMessage() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .mergeRefIntoBranch()
                .message("test-message-override-123")
                .branch(target)
                .fromRef(source)
                .returnConflictAsResult(returnConflictAsResult)
                .merge(),
        ImmutableList.of("test-message-override-123"));
  }

  @Test
  public void mergeMessageDefault() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .mergeRefIntoBranch()
                .branch(target)
                .fromRef(source)
                .returnConflictAsResult(returnConflictAsResult)
                .merge(),
        ImmutableList.of(
            "test-commit-1\n---------------------------------------------\ntest-commit-2"));
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1) // V2 does not allow squashed transplants
  public void transplantMessageSquashed() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .transplantCommitsIntoBranch()
                .message("test-message-override-123")
                .branch(target)
                .fromRefName(source.getName())
                .keepIndividualCommits(false)
                .hashesToTransplant(
                    ImmutableList.of(
                        Objects.requireNonNull(committed1.getHash()),
                        Objects.requireNonNull(committed2.getHash())))
                .returnConflictAsResult(returnConflictAsResult)
                .transplant(),
        ImmutableList.of("test-message-override-123"));
  }

  @Test
  @NessieApiVersions(versions = NessieApiVersion.V1) // V2 does not allow squashed transplants
  public void transplantMessageSingle() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .transplantCommitsIntoBranch()
                .message("test-message-override-123")
                .branch(target)
                .fromRefName(source.getName())
                .hashesToTransplant(ImmutableList.of(Objects.requireNonNull(committed1.getHash())))
                .returnConflictAsResult(returnConflictAsResult)
                .transplant(),
        ImmutableList.of("test-message-override-123"));
  }

  @Test
  public void transplantMessageOverrideMultiple() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            getApi()
                .transplantCommitsIntoBranch()
                .message("ignored-message-override")
                .keepIndividualCommits(true)
                .branch(target)
                .fromRefName(source.getName())
                .hashesToTransplant(
                    ImmutableList.of(
                        Objects.requireNonNull(committed1.getHash()),
                        Objects.requireNonNull(committed2.getHash())))
                .returnConflictAsResult(returnConflictAsResult)
                .transplant(),
        // Note: the expected messages are given in the commit log order (newest to oldest)
        ImmutableList.of("test-commit-2", "test-commit-1"));
  }

  private void testMergeTransplantMessage(
      MergeTransplantActor actor, Collection<String> expectedMessages)
      throws BaseNessieClientServerException {
    Branch target = createBranch("merge-transplant-msg-target");

    // Common ancestor
    target =
        getApi()
            .commitMultipleOperations()
            .branch(target)
            .commitMeta(CommitMeta.fromMessage("test-root"))
            .operation(
                Put.of(
                    ContentKey.of("irrelevant-to-this-test"),
                    IcebergTable.of("something", 42, 43, 44, 45)))
            .commit();

    Branch source = createBranch("merge-transplant-msg-source");

    ContentKey key1 = ContentKey.of("test-key1");
    ContentKey key2 = ContentKey.of("test-key2");

    source =
        getApi()
            .commitMultipleOperations()
            .branch(source)
            .commitMeta(CommitMeta.fromMessage("test-commit-1"))
            .operation(Put.of(key1, IcebergTable.of("table1", 42, 43, 44, 45)))
            .commit();

    Branch firstCommitOnSource = source;

    source =
        getApi()
            .commitMultipleOperations()
            .branch(source)
            .commitMeta(CommitMeta.fromMessage("test-commit-2"))
            .operation(Put.of(key2, IcebergTable.of("table2", 42, 43, 44, 45)))
            .commit();

    actor.act(target, source, firstCommitOnSource, source, false);

    Stream<LogEntry> logStream = getApi().getCommitLog().refName(target.getName()).stream();
    soft.assertThat(logStream.limit(expectedMessages.size()))
        .isNotEmpty()
        .extracting(e -> e.getCommitMeta().getMessage())
        .containsExactlyElementsOf(expectedMessages);
  }

  @ParameterizedTest
  @EnumSource(
      value = ReferenceMode.class,
      mode = Mode.EXCLUDE,
      names = "NAME_ONLY") // merge requires the hash
  public void mergeWithNamespaces(ReferenceMode refMode) throws BaseNessieClientServerException {
    Branch root = createBranch("root");

    // common ancestor commit
    ContentKey something = ContentKey.of("something");
    root =
        getApi()
            .commitMultipleOperations()
            .branchName(root.getName())
            .hash(root.getHash())
            .commitMeta(CommitMeta.fromMessage("test-branch1"))
            .operation(Put.of(something, IcebergTable.of("something", 42, 43, 44, 45)))
            .commit();

    Branch base = createBranch("merge-base", root);
    Branch branch = createBranch("merge-branch", root);

    // create the same namespace on both branches
    Namespace ns = Namespace.parse("a.b.c");
    getApi().createNamespace().namespace(ns).refName(base.getName()).create();
    getApi().createNamespace().namespace(ns).refName(branch.getName()).create();

    base = (Branch) getApi().getReference().refName(base.getName()).get();
    branch = (Branch) getApi().getReference().refName(branch.getName()).get();

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
    soft.assertThat(committed1.getHash()).isNotNull();

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
    soft.assertThat(committed2.getHash()).isNotNull();

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
        .keepIndividualCommits(false)
        .merge();

    LogResponse log =
        getApi().getCommitLog().refName(base.getName()).untilHash(base.getHash()).get();
    soft.assertThat(
            log.getLogEntries().stream()
                .map(LogEntry::getCommitMeta)
                .map(CommitMeta::getMessage)
                .findFirst())
        .isPresent()
        .hasValueSatisfying(v -> assertThat(v).contains("test-branch1"))
        .hasValueSatisfying(v -> assertThat(v).contains("test-branch2"));

    soft.assertThat(
            getApi().getEntries().refName(base.getName()).get().getEntries().stream()
                .map(Entry::getName))
        .containsExactlyInAnyOrder(something, key1, key2, ContentKey.of(ns.getElements()));

    soft.assertThat(getApi().getNamespace().refName(base.getName()).namespace(ns).get())
        .isNotNull();
  }

  @Test
  public void mergeWithCustomModes() throws BaseNessieClientServerException {
    MergeReferenceBuilder merge = getApi().mergeRefIntoBranch();
    testMergeTransplantWithCustomModes(
        merge,
        (target, source, committed1, committed2, returnConflictAsResult) ->
            merge
                .branch(target)
                .fromRef(source)
                .returnConflictAsResult(returnConflictAsResult)
                .merge());
  }

  @Test
  public void transplantWithCustomModes() throws BaseNessieClientServerException {
    TransplantCommitsBuilder transplant = getApi().transplantCommitsIntoBranch();
    testMergeTransplantWithCustomModes(
        transplant,
        (target, source, committed1, committed2, returnConflictAsResult) ->
            transplant
                .branch(target)
                .fromRefName(source.getName())
                .hashesToTransplant(
                    ImmutableList.of(
                        Objects.requireNonNull(committed1.getHash()),
                        Objects.requireNonNull(committed2.getHash())))
                .returnConflictAsResult(returnConflictAsResult)
                .transplant());
  }

  private <B extends MergeTransplantBuilder<B>> void testMergeTransplantWithCustomModes(
      B opBuilder, MergeTransplantActor actor) throws BaseNessieClientServerException {
    Branch target = createBranch("target");

    // Common ancestor
    target =
        getApi()
            .commitMultipleOperations()
            .branch(target)
            .commitMeta(CommitMeta.fromMessage("test-root"))
            .operation(
                Put.of(
                    ContentKey.of("irrelevant-to-this-test"),
                    IcebergTable.of("something", 42, 43, 44, 45)))
            .commit();

    Branch branch = createBranch("test-branch", target);

    ContentKey key1 = ContentKey.of("both-added1");
    ContentKey key2 = ContentKey.of("both-added2");
    ContentKey key3 = ContentKey.of("branch-added");
    target =
        getApi()
            .commitMultipleOperations()
            .branch(target)
            .commitMeta(CommitMeta.fromMessage("test-main"))
            .operation(Put.of(key1, IcebergTable.of("main-table1", 42, 43, 44, 45)))
            .operation(Put.of(key2, IcebergTable.of("main-table1", 42, 43, 44, 45)))
            .commit();

    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("test-fork"))
            .operation(Put.of(key1, IcebergTable.of("branch-table1", 42, 43, 44, 45)))
            .operation(Put.of(key2, IcebergTable.of("branch-table2", 42, 43, 44, 45)))
            .commit();

    Branch firstCommitOnBranch = branch;

    branch =
        getApi()
            .commitMultipleOperations()
            .branch(branch)
            .commitMeta(CommitMeta.fromMessage("test-fork"))
            .operation(Put.of(key3, IcebergTable.of("branch-no-conflict", 42, 43, 44, 45)))
            .commit();

    opBuilder
        .defaultMergeMode(MergeBehavior.FORCE)
        .mergeMode(key1, MergeBehavior.DROP)
        .mergeMode(key3, MergeBehavior.NORMAL);

    MergeResponse response = actor.act(target, branch, firstCommitOnBranch, branch, false);

    soft.assertThat(response.getDetails())
        .asInstanceOf(list(ContentKeyDetails.class))
        .extracting(
            ContentKeyDetails::getKey,
            ContentKeyDetails::getConflictType,
            ContentKeyDetails::getMergeBehavior)
        .containsExactlyInAnyOrder(
            tuple(key1, ContentKeyConflict.NONE, MergeBehavior.DROP),
            tuple(key2, ContentKeyConflict.NONE, MergeBehavior.FORCE),
            tuple(key3, ContentKeyConflict.NONE, MergeBehavior.NORMAL));

    soft.assertThat(
            getApi()
                .getContent()
                .refName(target.getName())
                .hashOnRef(response.getResultantTargetHash())
                .key(key1)
                .key(key2)
                .key(key3)
                .get()
                .entrySet())
        .extracting(Map.Entry::getKey, e -> ((IcebergTable) e.getValue()).getMetadataLocation())
        .containsExactlyInAnyOrder(
            tuple(key1, "main-table1"),
            tuple(key2, "branch-table2"),
            tuple(key3, "branch-no-conflict"));
  }
}
