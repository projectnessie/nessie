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
package org.projectnessie.events.service;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import java.security.Principal;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.ImmutableTransplantEvent;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.TransplantResult;

class TestEventFactory {

  UUID uuid = UUID.randomUUID();
  Instant now = Instant.now();

  Commit commit =
      ImmutableCommit.builder()
          .hash(Hash.of("deadbeef"))
          .parentHash(Hash.of("cafebabe"))
          .commitMeta(
              ImmutableCommitMeta.builder()
                  .committer("committer")
                  .author("author")
                  .message("message")
                  .commitTime(now)
                  .authorTime(now)
                  .build())
          .build();

  EventConfig config =
      new EventConfig() {
        @Override
        public Map<String, String> getStaticProperties() {
          return ImmutableMap.of("key", "value");
        }

        @Override
        public Supplier<UUID> getIdGenerator() {
          return () -> uuid;
        }

        @Override
        public Clock getClock() {
          return Clock.fixed(now, UTC);
        }
      };

  @ParameterizedTest
  @MethodSource("principals")
  void newCommitEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    Event actual = ef.newCommitEvent(commit, BranchName.of("branch1"), "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableCommitEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .hashBefore("cafebabe")
                .hashAfter("deadbeef")
                .reference(Branch.of("branch1", "deadbeef"))
                .commitMeta(
                    ImmutableCommitMeta.builder()
                        .message("message")
                        .addAllAuthors("author")
                        .committer("committer")
                        .authorTime(now)
                        .commitTime(now)
                        .build())
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newMergeEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    MergeResult result =
        MergeResult.builder()
            .sourceRef(BranchName.of("branch1"))
            .sourceHash(Hash.of("11111111"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("cafebabe")) // hash before
            .resultantTargetHash(Hash.of("deadbeef")) // hash after
            .commonAncestor(Hash.of("0000"))
            .addCreatedCommits(commit)
            .build();
    Event actual = ef.newMergeEvent(result, "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableMergeEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .commonAncestorHash("0000")
                .hashBefore("cafebabe")
                .hashAfter("deadbeef")
                .sourceHash(Hash.of("11111111").asString())
                .sourceReference(Branch.of("branch1", "11111111"))
                .targetReference(Branch.of("branch2", "deadbeef"))
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newTransplantEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    TransplantResult result =
        TransplantResult.builder()
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("cafebabe")) // hash before
            .resultantTargetHash(Hash.of("deadbeef")) // hash after
            .addCreatedCommits(commit)
            .addCreatedCommits(commit)
            .addCreatedCommits(commit)
            .build();
    Event actual = ef.newTransplantEvent(result, "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableTransplantEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .hashBefore("cafebabe")
                .hashAfter("deadbeef")
                .commitCount(3)
                .targetReference(Branch.of("branch2", "deadbeef"))
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newReferenceCreatedEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    ReferenceCreatedResult result =
        ImmutableReferenceCreatedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("cafebabe"))
            .build();
    Event actual = ef.newReferenceCreatedEvent(result, "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceCreatedEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .reference(Branch.of("branch1", "cafebabe"))
                .hashAfter("cafebabe")
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newReferenceUpdatedEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    ReferenceAssignedResult result =
        ImmutableReferenceAssignedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .previousHash(Hash.of("cafebabe"))
            .currentHash(Hash.of("deadbeef"))
            .build();
    Event actual = ef.newReferenceUpdatedEvent(result, "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceUpdatedEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .reference(Branch.of("branch1", "deadbeef"))
                .hashBefore("cafebabe")
                .hashAfter("deadbeef")
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newReferenceDeletedEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    ReferenceDeletedResult result =
        ImmutableReferenceDeletedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("cafebabe"))
            .build();
    Event actual = ef.newReferenceDeletedEvent(result, "repo1", user);
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceDeletedEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .reference(Branch.of("branch1", "cafebabe"))
                .hashBefore("cafebabe")
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newContentStoredEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    Content table = IcebergTable.of("location", 1, 2, 3, 4, "table1");
    Event actual =
        ef.newContentStoredEvent(
            BranchName.of("branch1"),
            Hash.of("cafebabe"),
            now,
            ContentKey.of("foo.bar.table1"),
            table,
            "repo1",
            user);
    assertThat(actual)
        .isEqualTo(
            ImmutableContentStoredEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .reference(Branch.of("branch1", "cafebabe"))
                .hash("cafebabe")
                .commitCreationTimestamp(now)
                .contentKey(ContentKey.of("foo.bar.table1"))
                .content(table)
                .build());
  }

  @ParameterizedTest
  @MethodSource("principals")
  void newContentRemovedEvent(Principal user, String expectedInitiator) {
    EventFactory ef = new EventFactory(config);
    Event actual =
        ef.newContentRemovedEvent(
            BranchName.of("branch1"),
            Hash.of("cafebabe"),
            now,
            ContentKey.of("foo.bar.table1"),
            "repo1",
            user);
    assertThat(actual)
        .isEqualTo(
            ImmutableContentRemovedEvent.builder()
                .id(uuid)
                .eventInitiator(Optional.ofNullable(expectedInitiator))
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .putProperty("key", "value")
                .reference(Branch.of("branch1", "cafebabe"))
                .hash("cafebabe")
                .commitCreationTimestamp(now)
                .contentKey(ContentKey.of("foo.bar.table1"))
                .build());
  }

  public static Stream<Arguments> principals() {
    // When auth is disabled, Quarkus will create a Principal with an empty name,
    // see io.quarkus.security.runtime.AnonymousIdentityProvider.
    // Here we test that the event factory can handle this, and also:
    // a Principal with a null name, and a null Principal.
    return Stream.of(
        // auth enabled: normal non-null principal with non-empty name
        Arguments.of((Principal) () -> "alice", "alice"),
        // auth disabled: principal with empty name
        Arguments.of((Principal) () -> "", null),
        // auth disabled: principal with null name
        Arguments.of((Principal) () -> null, null),
        // null Principal
        Arguments.of(null, null));
  }
}
