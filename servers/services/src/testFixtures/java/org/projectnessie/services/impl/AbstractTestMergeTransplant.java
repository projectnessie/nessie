/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.services.impl;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.FetchOption.MINIMAL;
import static org.projectnessie.model.MergeBehavior.DROP;
import static org.projectnessie.model.MergeBehavior.FORCE;
import static org.projectnessie.model.MergeBehavior.NORMAL;
import static org.projectnessie.versioned.RequestMeta.API_READ;
import static org.projectnessie.versioned.RequestMeta.API_WRITE;

import com.google.common.collect.ImmutableList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ContentResponse;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.MergeKeyBehavior;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.MergeResponse.ContentKeyConflict;
import org.projectnessie.model.MergeResponse.ContentKeyDetails;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractTestMergeTransplant extends BaseTestServiceImpl {

  private static final ContentKey KEY_1 = ContentKey.of("both-added1");
  private static final ContentKey KEY_2 = ContentKey.of("both-added2");
  private static final ContentKey KEY_3 = ContentKey.of("branch-added");

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void transplant(boolean withDetachedCommit) throws BaseNessieClientServerException {
    testTransplant(withDetachedCommit);
  }

  private void testTransplant(boolean withDetachedCommit) throws BaseNessieClientServerException {
    mergeTransplant(
        false,
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .transplantCommitsIntoBranch(
                    target.getName(),
                    target.getHash(),
                    null,
                    ImmutableList.of(
                        requireNonNull(committed1.getHash()), requireNonNull(committed2.getHash())),
                    maybeAsDetachedName(withDetachedCommit, source),
                    emptyList(),
                    NORMAL,
                    false,
                    false,
                    returnConflictAsResult),
        withDetachedCommit,
        false);
  }

  @ParameterizedTest
  @EnumSource(names = {"UNCHANGED", "DETACHED"}) // hash is required
  public void merge(ReferenceMode refMode) throws BaseNessieClientServerException {
    mergeTransplant(
        true,
        (target, source, committed1, committed2, returnConflictAsResult) -> {
          Reference fromRef = refMode.transform(committed2);
          return treeApi()
              .mergeRefIntoBranch(
                  target.getName(),
                  target.getHash(),
                  fromRef.getName(),
                  fromRef.getHash(),
                  null,
                  emptyList(),
                  NORMAL,
                  false,
                  false,
                  returnConflictAsResult);
        },
        refMode == ReferenceMode.DETACHED,
        true);
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

  @SuppressWarnings("deprecation")
  private void mergeTransplant(
      boolean verifyAdditionalParents,
      MergeTransplantActor actor,
      boolean detached,
      boolean isMerge)
      throws BaseNessieClientServerException {
    Branch root = createBranch("root");

    root =
        commit(
                root,
                fromMessage("root"),
                Put.of(ContentKey.of("other"), IcebergTable.of("/dev/null", 42, 42, 42, 42)))
            .getTargetBranch();

    Branch target = createBranch("base", root);
    Branch source = createBranch("branch", root);

    ContentKey key1 = ContentKey.of("key1");
    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    ContentKey key2 = ContentKey.of("key2");
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    Branch committed1 =
        commit(source, fromMessage("test-branch1"), Put.of(key1, table1)).getTargetBranch();
    soft.assertThat(committed1.getHash()).isNotNull();

    table1 =
        (IcebergTable)
            contentApi()
                .getContent(key1, committed1.getName(), committed1.getHash(), false, API_READ)
                .getContent();

    Branch committed2 =
        commit(
                source.getName(),
                committed1.getHash(),
                fromMessage("test-branch2"),
                Put.of(key1, table1))
            .getTargetBranch();
    soft.assertThat(committed2.getHash()).isNotNull();

    int commitCount = 2;

    List<LogEntry> logBranch = commitLog(source.getName(), MINIMAL, source.getHash(), null, null);

    Branch baseHead =
        commit(target, fromMessage("test-main"), Put.of(key2, table2)).getTargetBranch();

    MergeResponse response = actor.act(target, source, committed1, committed2, false);
    Reference newHead = mergeWentFine(target, source, key1, baseHead, response);

    // try again --> conflict

    if (isMerge) {
      // New storage model allows "merging the same branch again". If nothing changed, it returns a
      // successful, but not-applied merge-response. This request is effectively a merge without any
      // commits to merge, reported as "successful".
      soft.assertThat(actor.act(target, source, committed1, committed2, false))
          .extracting(
              MergeResponse::getCommonAncestor,
              MergeResponse::getEffectiveTargetHash,
              MergeResponse::getResultantTargetHash,
              MergeResponse::wasApplied,
              MergeResponse::wasSuccessful)
          .containsExactly(committed2.getHash(), newHead.getHash(), newHead.getHash(), false, true);
    }

    List<LogEntry> log = commitLog(target.getName(), MINIMAL, target.getHash(), null, null);
    if (!isMerge) {
      soft.assertThat(log.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .containsExactly("test-branch2", "test-branch1", "test-main", "root");
    } else {
      soft.assertThat(log.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage))
          .hasSize(3)
          .first(InstanceOfAssertFactories.STRING)
          .isEqualTo(
              format(
                  "%s %s at %s into %s at %s",
                  "Merged",
                  detached ? "DETACHED" : source.getName(),
                  committed2.getHash(),
                  target.getName(),
                  target.getHash()));
    }

    // Verify that the commit-timestamp was updated
    List<LogEntry> logOfMerged =
        commitLog(target.getName()).stream().limit(commitCount).collect(Collectors.toList());
    soft.assertThat(
            logOfMerged.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getCommitTime))
        .isNotEqualTo(
            logBranch.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getCommitTime));

    soft.assertThat(entries(target.getName(), null).stream().map(e -> e.getName().getName()))
        .containsExactlyInAnyOrder("other", "key1", "key2");

    if (verifyAdditionalParents) {
      soft.assertThat(logOfMerged)
          .first()
          .extracting(LogEntry::getAdditionalParents)
          .asInstanceOf(list(String.class))
          .isEmpty();
      soft.assertThat(logOfMerged)
          .first()
          .extracting(LogEntry::getCommitMeta)
          .extracting(CommitMeta::getParentCommitHashes)
          .asInstanceOf(list(String.class))
          .containsExactly(baseHead.getHash(), committed2.getHash());
    }
  }

  private Reference mergeWentFine(
      Branch target, Branch source, ContentKey key1, Branch baseHead, MergeResponse response)
      throws NessieNotFoundException {
    Reference newHead = getReference(target.getName());
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
        .singleElement()
        .satisfies(
            details -> {
              assertThat(details.getKey()).isEqualTo(key1);
              assertThat(details.getConflict()).isNull();
              assertThat(details.getMergeBehavior()).isEqualTo(NORMAL);
            });
    return newHead;
  }

  @Test
  public void mergeMessage() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .mergeRefIntoBranch(
                    target.getName(),
                    target.getHash(),
                    source.getName(),
                    source.getHash(),
                    CommitMeta.fromMessage("test-message-override-123"),
                    emptyList(),
                    NORMAL,
                    false,
                    false,
                    returnConflictAsResult),
        ImmutableList.of("test-message-override-123"));
  }

  @Test
  public void mergeMessageDefault() throws BaseNessieClientServerException {
    Branch target = createBranch("merge-transplant-msg-target");

    // Common ancestor
    target =
        commit(
                target,
                fromMessage("test-root"),
                Put.of(
                    ContentKey.of("irrelevant-to-this-test"),
                    IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch source = createBranch("merge-transplant-msg-source", target);

    ContentKey key1 = ContentKey.of("test-key1");
    ContentKey key2 = ContentKey.of("test-key2");

    source =
        commit(
                source,
                fromMessage("test-commit-1"),
                Put.of(key1, IcebergTable.of("table1", 42, 43, 44, 45)))
            .getTargetBranch();

    source =
        commit(
                source,
                fromMessage("test-commit-2"),
                Put.of(key2, IcebergTable.of("table2", 42, 43, 44, 45)))
            .getTargetBranch();

    treeApi()
        .mergeRefIntoBranch(
            target.getName(),
            target.getHash(),
            source.getName(),
            source.getHash(),
            null,
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    soft.assertThat(commitLog(target.getName()).stream().limit(1))
        .isNotEmpty()
        .extracting(e -> e.getCommitMeta().getMessage())
        .containsExactly(
            format(
                "Merged %s at %s into %s at %s",
                source.getName(), source.getHash(), target.getName(), target.getHash()));
  }

  @Test
  public void transplantMessage() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .transplantCommitsIntoBranch(
                    target.getName(),
                    target.getHash(),
                    CommitMeta.fromMessage("test-message-override-123"),
                    ImmutableList.of(requireNonNull(committed1.getHash())),
                    source.getName(),
                    emptyList(),
                    NORMAL,
                    false,
                    false,
                    returnConflictAsResult),
        ImmutableList.of("test-message-override-123"));
  }

  @Test
  public void transplantMessageOverrideMultiple() throws BaseNessieClientServerException {
    testMergeTransplantMessage(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .transplantCommitsIntoBranch(
                    target.getName(),
                    target.getHash(),
                    CommitMeta.fromMessage("ignored-message-override"),
                    ImmutableList.of(
                        requireNonNull(committed1.getHash()), requireNonNull(committed2.getHash())),
                    source.getName(),
                    emptyList(),
                    NORMAL,
                    false,
                    false,
                    returnConflictAsResult),
        // Note: the expected messages are given in the commit log order (newest to oldest)
        ImmutableList.of("test-commit-2", "test-commit-1"));
  }

  private void testMergeTransplantMessage(
      MergeTransplantActor actor, Collection<String> expectedMessages)
      throws BaseNessieClientServerException {
    Branch target = createBranch("merge-transplant-msg-target");

    // Common ancestor
    target =
        commit(
                target,
                fromMessage("test-root"),
                Put.of(
                    ContentKey.of("irrelevant-to-this-test"),
                    IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch source = createBranch("merge-transplant-msg-source", target);

    ContentKey key1 = ContentKey.of("test-key1");
    ContentKey key2 = ContentKey.of("test-key2");

    source =
        commit(
                source,
                fromMessage("test-commit-1"),
                Put.of(key1, IcebergTable.of("table1", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch firstCommitOnSource = source;

    source =
        commit(
                source,
                fromMessage("test-commit-2"),
                Put.of(key2, IcebergTable.of("table2", 42, 43, 44, 45)))
            .getTargetBranch();

    actor.act(target, source, firstCommitOnSource, source, false);

    soft.assertThat(commitLog(target.getName()).stream().limit(expectedMessages.size()))
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
        commit(
                root,
                fromMessage("test-branch1"),
                Put.of(something, IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch base = createBranch("merge-base", root);
    Branch branch = createBranch("merge-branch", root);

    // create the same namespace on both branches
    Namespace ns = Namespace.parse("a.b.c");
    base = ensureNamespacesForKeysExist(base, ns.toContentKey());
    branch = ensureNamespacesForKeysExist(branch, ns.toContentKey());
    namespaceApi().createNamespace(base.getName(), ns, API_WRITE);
    namespaceApi().createNamespace(branch.getName(), ns, API_WRITE);

    base = (Branch) getReference(base.getName());
    branch = (Branch) getReference(branch.getName());

    IcebergTable table1 = IcebergTable.of("table1", 42, 42, 42, 42);
    IcebergTable table2 = IcebergTable.of("table2", 43, 43, 43, 43);

    ContentKey key1 = ContentKey.of(ns, "key1");
    ContentKey key2 = ContentKey.of(ns, "key2");
    Branch committed1 =
        commit(branch, fromMessage("test-branch1"), Put.of(key1, table1)).getTargetBranch();
    soft.assertThat(committed1.getHash()).isNotNull();

    table1 =
        (IcebergTable)
            contentApi()
                .getContent(key1, committed1.getName(), committed1.getHash(), false, API_READ)
                .getContent();

    Branch committed2 =
        commit(committed1, fromMessage("test-branch2"), Put.of(key1, table1)).getTargetBranch();
    soft.assertThat(committed2.getHash()).isNotNull();

    commit(base, fromMessage("test-main"), Put.of(key2, table2));

    Reference fromRef = refMode.transform(committed2);
    treeApi()
        .mergeRefIntoBranch(
            base.getName(),
            base.getHash(),
            fromRef.getName(),
            fromRef.getHash(),
            null,
            emptyList(),
            NORMAL,
            false,
            false,
            false);

    List<LogEntry> log = commitLog(base.getName(), MINIMAL, base.getHash(), null, null);
    soft.assertThat(
            log.stream().map(LogEntry::getCommitMeta).map(CommitMeta::getMessage).findFirst())
        .get()
        .isEqualTo(
            format(
                "Merged %s at %s into %s at %s",
                fromRef.getName(), fromRef.getHash(), base.getName(), base.getHash()));

    soft.assertThat(
            withoutNamespaces(entries(base.getName(), null)).stream()
                .map(EntriesResponse.Entry::getName))
        .containsExactlyInAnyOrder(something, key1, key2);

    soft.assertThat(namespaceApi().getNamespace(base.getName(), null, ns)).isNotNull();
  }

  @Test
  public void mergeWithCustomModes() throws BaseNessieClientServerException {
    testMergeTransplantWithCustomModes(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .mergeRefIntoBranch(
                    target.getName(),
                    target.getHash(),
                    source.getName(),
                    source.getHash(),
                    null,
                    asList(MergeKeyBehavior.of(KEY_1, DROP), MergeKeyBehavior.of(KEY_3, NORMAL)),
                    FORCE,
                    false,
                    false,
                    returnConflictAsResult));
  }

  @Test
  public void transplantWithCustomModes() throws BaseNessieClientServerException {
    testMergeTransplantWithCustomModes(
        (target, source, committed1, committed2, returnConflictAsResult) ->
            treeApi()
                .transplantCommitsIntoBranch(
                    target.getName(),
                    target.getHash(),
                    null,
                    ImmutableList.of(
                        requireNonNull(committed1.getHash()), requireNonNull(committed2.getHash())),
                    source.getName(),
                    asList(MergeKeyBehavior.of(KEY_1, DROP), MergeKeyBehavior.of(KEY_3, NORMAL)),
                    FORCE,
                    false,
                    false,
                    returnConflictAsResult));
  }

  @SuppressWarnings("deprecation")
  private void testMergeTransplantWithCustomModes(MergeTransplantActor actor)
      throws BaseNessieClientServerException {
    Branch target = createBranch("target");

    // Common ancestor
    target =
        commit(
                target,
                fromMessage("test-root"),
                Put.of(
                    ContentKey.of("irrelevant-to-this-test"),
                    IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch branch = createBranch("test-branch", target);

    target =
        commit(
                target,
                fromMessage("test-main"),
                Put.of(KEY_1, IcebergTable.of("main-table1", 42, 43, 44, 45)),
                Put.of(KEY_2, IcebergTable.of("main-table1", 42, 43, 44, 45)))
            .getTargetBranch();

    branch =
        commit(
                branch,
                fromMessage("test-fork"),
                Put.of(KEY_1, IcebergTable.of("branch-table1", 42, 43, 44, 45)),
                Put.of(KEY_2, IcebergTable.of("branch-table2", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch firstCommitOnBranch = branch;

    branch =
        commit(
                branch,
                fromMessage("test-fork"),
                Put.of(KEY_3, IcebergTable.of("branch-no-conflict", 42, 43, 44, 45)))
            .getTargetBranch();

    MergeResponse response = actor.act(target, branch, firstCommitOnBranch, branch, false);

    soft.assertThat(response.getDetails())
        .asInstanceOf(list(ContentKeyDetails.class))
        .extracting(
            ContentKeyDetails::getKey,
            ContentKeyDetails::getConflictType,
            ContentKeyDetails::getMergeBehavior)
        .containsExactlyInAnyOrder(
            tuple(KEY_1, ContentKeyConflict.NONE, DROP),
            tuple(KEY_2, ContentKeyConflict.NONE, FORCE),
            tuple(KEY_3, ContentKeyConflict.NONE, NORMAL));

    soft.assertThat(
            contents(target.getName(), response.getResultantTargetHash(), KEY_1, KEY_2, KEY_3)
                .entrySet())
        .extracting(Map.Entry::getKey, e -> ((IcebergTable) e.getValue()).getMetadataLocation())
        .containsExactlyInAnyOrder(
            tuple(KEY_1, "main-table1"),
            tuple(KEY_2, "branch-table2"),
            tuple(KEY_3, "branch-no-conflict"));
  }

  @Test
  public void mergeRecreateTableNoConflict() throws BaseNessieClientServerException {

    MergeRecreateTableSetup setup = setupMergeRecreateTable();

    MergeResponse mergeResponse =
        treeApi()
            .mergeRefIntoBranch(
                setup.root.getName(),
                setup.root.getHash(),
                setup.work.getName(),
                setup.work.getHash(),
                fromMessage("merge-recreate"),
                emptyList(),
                NORMAL,
                Boolean.FALSE,
                Boolean.FALSE,
                Boolean.FALSE);

    Branch rootAfterMerge =
        Branch.of(mergeResponse.getTargetBranch(), mergeResponse.getResultantTargetHash());
    soft.assertThat(rootAfterMerge).isNotEqualTo(setup.root);

    ContentResponse tableOnRootAfterMerge =
        contentApi()
            .getContent(
                setup.key, rootAfterMerge.getName(), rootAfterMerge.getHash(), false, API_READ);

    soft.assertThat(setup.tableOnWork.getContent().getId())
        .isEqualTo(tableOnRootAfterMerge.getContent().getId());

    // add a commit modifying the table to ensure the content key is OK
    commit(
        rootAfterMerge,
        fromMessage("add-to-merged-table"),
        Put.of(
            setup.key,
            IcebergTable.of("something even more different", 43, 44, 45, 47)
                .withId(tableOnRootAfterMerge.getContent().getId())));
  }

  @Test
  public void mergeRecreateTableConflict() throws BaseNessieClientServerException {

    MergeRecreateTableSetup setup = setupMergeRecreateTable();

    Branch root =
        commit(
                setup.root,
                fromMessage("update-table-on-root"),
                Put.of(
                    setup.key,
                    IcebergTable.of("something even more different", 43, 44, 45, 46)
                        .withId(setup.tableOnRoot.getContent().getId())))
            .getTargetBranch();
    soft.assertThat(root).isNotEqualTo(setup.root);

    assertThatThrownBy(
            () ->
                treeApi()
                    .mergeRefIntoBranch(
                        root.getName(),
                        root.getHash(),
                        setup.work.getName(),
                        setup.work.getHash(),
                        fromMessage("merge-recreate-conflict"),
                        emptyList(),
                        NORMAL,
                        Boolean.FALSE,
                        Boolean.FALSE,
                        Boolean.FALSE))
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessage("The following keys have been changed in conflict: '%s'", setup.key);
  }

  private MergeRecreateTableSetup setupMergeRecreateTable()
      throws NessieNotFoundException, NessieConflictException {
    Branch root = createBranch("root");
    Branch lastRoot = root;

    ContentKey key = ContentKey.of("my_table");

    root =
        commit(
                root,
                fromMessage("initial-create-table"),
                Put.of(key, IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();
    soft.assertThat(root).isNotEqualTo(lastRoot);

    ContentResponse tableOnRoot =
        contentApi().getContent(key, root.getName(), root.getHash(), false, API_READ);
    soft.assertThat(tableOnRoot.getEffectiveReference()).isEqualTo(root);

    Branch work = createBranch("recreateBranch", root);
    soft.assertThat(work.getHash()).isEqualTo(root.getHash());
    Branch lastWork = work;

    work = commit(work, fromMessage("drop-table"), Delete.of(key)).getTargetBranch();
    soft.assertThat(work).isNotEqualTo(lastWork);
    lastWork = work;

    work =
        commit(
                work,
                fromMessage("recreate-table"),
                Put.of(key, IcebergTable.of("something different", 42, 43, 44, 45)))
            .getTargetBranch();
    soft.assertThat(work).isNotEqualTo(lastWork);

    ContentResponse tableOnWork =
        contentApi().getContent(key, work.getName(), work.getHash(), false, API_READ);
    soft.assertThat(tableOnWork.getEffectiveReference()).isEqualTo(work);

    soft.assertThat(tableOnWork.getContent().getId())
        .isNotEqualTo(tableOnRoot.getContent().getId());
    return new MergeRecreateTableSetup(root, work, tableOnRoot, tableOnWork, key);
  }

  private static class MergeRecreateTableSetup {

    final Branch root;
    final Branch work;
    final ContentResponse tableOnRoot;
    final ContentResponse tableOnWork;
    final ContentKey key;

    MergeRecreateTableSetup(
        Branch root,
        Branch work,
        ContentResponse tableOnRoot,
        ContentResponse tableOnWork,
        ContentKey key) {
      this.root = root;
      this.work = work;
      this.tableOnRoot = tableOnRoot;
      this.tableOnWork = tableOnWork;
      this.key = key;
    }
  }

  @Test
  public void mergeConflictingKey() throws BaseNessieClientServerException {

    Branch root = createBranch("root");
    root =
        commit(
                root,
                fromMessage("common-ancestor"),
                Put.of(ContentKey.of("unrelated"), IcebergTable.of("unrelated", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch work = createBranch("work", root);

    ContentKey key = ContentKey.of("conflicting");

    root =
        commit(
                root,
                fromMessage("create-table-on-root"),
                Put.of(key, IcebergTable.of("something", 42, 43, 44, 45)))
            .getTargetBranch();

    work =
        commit(
                work,
                fromMessage("create-table-on-work"),
                Put.of(key, IcebergTable.of("something else", 42, 43, 44, 45)))
            .getTargetBranch();

    Branch lastRoot = root;
    Branch lastWork = work;

    assertThatThrownBy(
            () ->
                treeApi()
                    .mergeRefIntoBranch(
                        lastRoot.getName(),
                        lastRoot.getHash(),
                        lastWork.getName(),
                        lastWork.getHash(),
                        fromMessage("merge-conflicting-keys"),
                        emptyList(),
                        NORMAL,
                        Boolean.FALSE,
                        Boolean.FALSE,
                        Boolean.FALSE))
        .isInstanceOf(NessieReferenceConflictException.class)
        .hasMessage("The following keys have been changed in conflict: 'conflicting'");
  }
}
