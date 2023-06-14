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
package org.projectnessie.versioned.persist.tests;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.MetadataRewriter;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.MergeParams;
import org.projectnessie.versioned.persist.adapter.TransplantParams;
import org.projectnessie.versioned.persist.adapter.events.AdapterEvent;
import org.projectnessie.versioned.persist.adapter.events.AdapterEventConsumer;
import org.projectnessie.versioned.persist.adapter.events.CommitEvent;
import org.projectnessie.versioned.persist.adapter.events.MergeEvent;
import org.projectnessie.versioned.persist.adapter.events.OperationType;
import org.projectnessie.versioned.persist.adapter.events.ReferenceAssignedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceCreatedEvent;
import org.projectnessie.versioned.persist.adapter.events.ReferenceDeletedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryErasedEvent;
import org.projectnessie.versioned.persist.adapter.events.RepositoryInitializedEvent;
import org.projectnessie.versioned.persist.adapter.events.TransplantEvent;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/** Verifies handling of repo-description in the database-adapters. */
public abstract class AbstractEvents {

  protected AbstractEvents() {}

  @Test
  public void eventRepoInit(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter) {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    adapter.initializeRepo("main");
    assertThat(events.events).isEmpty();

    adapter.eraseRepo();

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(RepositoryErasedEvent.class))
        .extracting(RepositoryErasedEvent::getOperationType)
        .isEqualTo(OperationType.REPOSITORY_ERASED);
    events.events.clear();

    adapter.initializeRepo("main");

    assertThat(events.events)
        .asInstanceOf(list(AdapterEvent.class))
        .satisfiesExactly(
            repoInit ->
                assertThat(repoInit)
                    .asInstanceOf(type(RepositoryInitializedEvent.class))
                    .extracting(
                        RepositoryInitializedEvent::getOperationType,
                        RepositoryInitializedEvent::getDefaultBranch)
                    .containsExactly(OperationType.REPOSITORY_INITIALIZED, "main"),
            createRef ->
                assertThat(createRef)
                    .asInstanceOf(type(ReferenceCreatedEvent.class))
                    .extracting(
                        ReferenceCreatedEvent::getOperationType,
                        ReferenceCreatedEvent::getCurrentHash,
                        ReferenceCreatedEvent::getRef)
                    .containsExactly(
                        OperationType.CRETE_REF, adapter.noAncestorHash(), BranchName.of("main")));
  }

  @Test
  public void eventCreateRef(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName branch = BranchName.of("events-create");

    adapter.create(branch, adapter.noAncestorHash());
    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(ReferenceCreatedEvent.class))
        .extracting(
            ReferenceCreatedEvent::getOperationType,
            ReferenceCreatedEvent::getCurrentHash,
            ReferenceCreatedEvent::getRef)
        .containsExactly(OperationType.CRETE_REF, adapter.noAncestorHash(), branch);
  }

  @Test
  public void eventDeleteRef(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName branch = BranchName.of("events-delete");

    adapter.create(branch, adapter.noAncestorHash());

    events.events.clear();

    adapter.delete(branch, Optional.empty());

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(ReferenceDeletedEvent.class))
        .extracting(
            ReferenceDeletedEvent::getOperationType,
            ReferenceDeletedEvent::getCurrentHash,
            ReferenceDeletedEvent::getRef)
        .containsExactly(OperationType.DELETE_REF, adapter.noAncestorHash(), branch);
  }

  @Test
  public void eventAssignRef(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName branch = BranchName.of("events-assign");

    adapter.create(branch, adapter.noAncestorHash());

    KeyWithBytes put =
        KeyWithBytes.of(
            ContentKey.of("one-two"),
            ContentId.of("cid-events-assign"),
            (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
            DefaultStoreWorker.instance()
                .toStoreOnReferenceState(OnRefOnly.onRef("foo", "cid-events-assign")));

    ByteString meta = ByteString.copyFromUtf8("foo bar baz");

    Hash committed =
        adapter
            .commit(
                ImmutableCommitParams.builder()
                    .toBranch(branch)
                    .commitMetaSerialized(meta)
                    .addPuts(put)
                    .build())
            .getCommitHash();

    events.events.clear();

    adapter.assign(branch, Optional.empty(), adapter.noAncestorHash());

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(ReferenceAssignedEvent.class))
        .extracting(
            ReferenceAssignedEvent::getOperationType,
            ReferenceAssignedEvent::getCurrentHash,
            ReferenceAssignedEvent::getPreviousHash,
            ReferenceAssignedEvent::getRef)
        .containsExactly(OperationType.ASSIGN_REF, adapter.noAncestorHash(), committed, branch);
  }

  @Test
  public void eventCommit(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName branch = BranchName.of("events-commit");
    adapter.create(branch, adapter.noAncestorHash());

    events.events.clear();

    KeyWithBytes put =
        KeyWithBytes.of(
            ContentKey.of("one-two"),
            ContentId.of("cid-events-commit"),
            (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
            DefaultStoreWorker.instance()
                .toStoreOnReferenceState(OnRefOnly.onRef("foo", "cid-events-commit")));

    ByteString meta = ByteString.copyFromUtf8("foo bar baz");

    Hash committed =
        adapter
            .commit(
                ImmutableCommitParams.builder()
                    .toBranch(branch)
                    .commitMetaSerialized(meta)
                    .addPuts(put)
                    .build())
            .getCommitHash();

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(CommitEvent.class))
        .satisfies(
            commit ->
                assertThat(commit)
                    .extracting(
                        CommitEvent::getOperationType,
                        CommitEvent::getHash,
                        CommitEvent::getPreviousHash,
                        CommitEvent::getBranch)
                    .containsExactly(
                        OperationType.COMMIT, committed, adapter.noAncestorHash(), branch),
            commit ->
                assertThat(commit.getCommits())
                    .hasSize(1)
                    .last()
                    .extracting(
                        CommitLogEntry::getHash,
                        CommitLogEntry::getPuts,
                        CommitLogEntry::getMetadata)
                    .containsExactly(committed, singletonList(put), meta));
  }

  @Test
  public void eventMerge(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName main = BranchName.of("main");

    KeyWithBytes put =
        KeyWithBytes.of(
            ContentKey.of("one-two"),
            ContentId.of("cid-events-merge"),
            (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
            DefaultStoreWorker.instance()
                .toStoreOnReferenceState(OnRefOnly.onRef("foo", "cid-events-merge")));

    ByteString meta = ByteString.copyFromUtf8("merge me");

    BranchName source = BranchName.of("events-merge");
    adapter.create(source, adapter.noAncestorHash());
    Hash committed =
        adapter
            .commit(
                ImmutableCommitParams.builder()
                    .toBranch(source)
                    .commitMetaSerialized(meta)
                    .addPuts(put)
                    .build())
            .getCommitHash();

    events.events.clear();

    MetadataRewriter<ByteString> updater = createMetadataUpdater();
    MergeResult<CommitLogEntry> mergeResult =
        adapter.merge(
            MergeParams.builder()
                .mergeFromHash(committed)
                .fromRef(source)
                .toBranch(main)
                .updateCommitMetadata(updater)
                .build());

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(MergeEvent.class))
        .satisfies(
            merge ->
                assertThat(merge)
                    .extracting(
                        MergeEvent::getOperationType,
                        MergeEvent::getHash,
                        MergeEvent::getPreviousHash,
                        MergeEvent::getBranch)
                    .containsExactly(
                        OperationType.MERGE,
                        mergeResult.getResultantTargetHash(),
                        adapter.noAncestorHash(),
                        main),
            merge ->
                assertThat(merge.getCommits())
                    .hasSize(1)
                    .last()
                    .extracting(
                        CommitLogEntry::getHash,
                        CommitLogEntry::getPuts,
                        CommitLogEntry::getMetadata)
                    .containsExactly(
                        mergeResult.getResultantTargetHash(),
                        singletonList(put),
                        updater.rewriteSingle(meta)));
  }

  @Test
  public void eventTransplant(
      @NessieDbAdapter(initializeRepo = false, eventConsumer = EventCollector.class)
          AbstractDatabaseAdapter<?, ?> adapter)
      throws Exception {

    EventCollector events = (EventCollector) adapter.getEventConsumer();

    BranchName main = BranchName.of("main");

    KeyWithBytes put =
        KeyWithBytes.of(
            ContentKey.of("one-two"),
            ContentId.of("cid-events-transplant"),
            (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
            DefaultStoreWorker.instance()
                .toStoreOnReferenceState(OnRefOnly.onRef("foo", "cid-events-transplant")));

    ByteString meta = ByteString.copyFromUtf8("transplant me");

    BranchName source = BranchName.of("events-transplant");
    adapter.create(source, adapter.noAncestorHash());
    Hash committed =
        adapter
            .commit(
                ImmutableCommitParams.builder()
                    .toBranch(source)
                    .commitMetaSerialized(meta)
                    .addPuts(put)
                    .build())
            .getCommitHash();

    events.events.clear();

    MetadataRewriter<ByteString> updater = createMetadataUpdater();
    MergeResult<CommitLogEntry> mergeResult =
        adapter.transplant(
            TransplantParams.builder()
                .addSequenceToTransplant(committed)
                .updateCommitMetadata(updater)
                .fromRef(source)
                .toBranch(main)
                .build());

    assertThat(events.events)
        .hasSize(1)
        .last()
        .asInstanceOf(type(TransplantEvent.class))
        .satisfies(
            transplant ->
                assertThat(transplant)
                    .extracting(
                        TransplantEvent::getOperationType,
                        TransplantEvent::getHash,
                        TransplantEvent::getPreviousHash,
                        TransplantEvent::getBranch)
                    .containsExactly(
                        OperationType.TRANSPLANT,
                        mergeResult.getResultantTargetHash(),
                        adapter.noAncestorHash(),
                        main),
            tranplant ->
                assertThat(tranplant.getCommits())
                    .hasSize(1)
                    .last()
                    .extracting(
                        CommitLogEntry::getHash,
                        CommitLogEntry::getPuts,
                        CommitLogEntry::getMetadata)
                    .containsExactly(
                        mergeResult.getResultantTargetHash(),
                        singletonList(put),
                        updater.rewriteSingle(meta)));
  }

  static class EventCollector implements AdapterEventConsumer {

    final List<AdapterEvent> events = new ArrayList<>();

    @Override
    public void accept(AdapterEvent event) {
      events.add(event);
    }
  }

  private static MetadataRewriter<ByteString> createMetadataUpdater() {
    return new MetadataRewriter<ByteString>() {
      @Override
      public ByteString rewriteSingle(ByteString metadata) {
        return ByteString.copyFromUtf8(metadata.toStringUtf8() + " updated");
      }

      @Override
      public ByteString squash(List<ByteString> metadata, int numCommits) {
        return rewriteSingle(metadata.get(0));
      }
    };
  }
}
