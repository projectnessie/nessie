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
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.ImmutableCommitEvent;
import org.projectnessie.events.api.ImmutableCommitMeta;
import org.projectnessie.events.api.ImmutableContent;
import org.projectnessie.events.api.ImmutableContentRemovedEvent;
import org.projectnessie.events.api.ImmutableContentStoredEvent;
import org.projectnessie.events.api.ImmutableMergeEvent;
import org.projectnessie.events.api.ImmutableReference;
import org.projectnessie.events.api.ImmutableReferenceCreatedEvent;
import org.projectnessie.events.api.ImmutableReferenceDeletedEvent;
import org.projectnessie.events.api.ImmutableReferenceUpdatedEvent;
import org.projectnessie.events.api.ImmutableTransplantEvent;
import org.projectnessie.events.api.Reference;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.ResultType;

class TestEventFactory {

  UUID uuid = UUID.randomUUID();
  Instant now = Instant.now();

  ImmutableReference branch1 =
      ImmutableReference.builder()
          .type(Reference.BRANCH)
          .simpleName(BranchName.of("branch1").getName())
          .fullName("refs/heads/" + BranchName.of("branch1").getName())
          .build();
  ImmutableReference branch2 =
      ImmutableReference.builder()
          .type(Reference.BRANCH)
          .simpleName(BranchName.of("branch2").getName())
          .fullName("refs/heads/" + BranchName.of("branch2").getName())
          .build();
  Commit commit =
      ImmutableCommit.builder()
          .hash(Hash.of("5678"))
          .parentHash(Hash.of("1234"))
          .commitMeta(
              org.projectnessie.model.ImmutableCommitMeta.builder()
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

  @Test
  void newCommitEvent() {
    EventFactory ef = new EventFactory(config);
    Event actual = ef.newCommitEvent(commit, BranchName.of("branch1"), "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableCommitEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .hashBefore(Hash.of("1234").asString())
                .hashAfter(Hash.of("5678").asString())
                .sourceReference(branch1)
                .targetReference(branch1)
                .commitMeta(
                    ImmutableCommitMeta.builder()
                        .message("message")
                        .addAuthor("author")
                        .committer("committer")
                        .authorTime(now)
                        .commitTime(now)
                        .build())
                .build());
  }

  @Test
  void newMergeEvent() {
    EventFactory ef = new EventFactory(config);
    MergeResult<Commit> result =
        ImmutableMergeResult.<Commit>builder()
            .resultType(ResultType.MERGE)
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("1234")) // hash before
            .resultantTargetHash(Hash.of("5678")) // hash after
            .commonAncestor(Hash.of("0000"))
            .addCreatedCommits(commit)
            .build();
    Event actual = ef.newMergeEvent(result, "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableMergeEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .commonAncestorHash(Hash.of("0000").asString())
                .hashBefore(Hash.of("1234").asString())
                .hashAfter(Hash.of("5678").asString())
                .sourceReference(branch1)
                .targetReference(branch2)
                .build());
  }

  @Test
  void newTransplantEvent() {
    EventFactory ef = new EventFactory(config);
    MergeResult<Commit> result =
        ImmutableMergeResult.<Commit>builder()
            .resultType(ResultType.TRANSPLANT)
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("1234")) // hash before
            .resultantTargetHash(Hash.of("5678")) // hash after
            .addCreatedCommits(commit)
            .build();
    Event actual = ef.newTransplantEvent(result, "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableTransplantEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .hashBefore(Hash.of("1234").asString())
                .hashAfter(Hash.of("5678").asString())
                .sourceReference(branch1)
                .targetReference(branch2)
                .build());
  }

  @Test
  void newReferenceCreatedEvent() {
    EventFactory ef = new EventFactory(config);
    ReferenceCreatedResult result =
        ImmutableReferenceCreatedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("1234"))
            .build();
    Event actual = ef.newReferenceCreatedEvent(result, "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceCreatedEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .reference(branch1)
                .hashAfter(Hash.of("1234").asString())
                .build());
  }

  @Test
  void newReferenceUpdatedEvent() {
    EventFactory ef = new EventFactory(config);
    ReferenceAssignedResult result =
        ImmutableReferenceAssignedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .previousHash(Hash.of("1234"))
            .currentHash(Hash.of("5678"))
            .build();
    Event actual = ef.newReferenceUpdatedEvent(result, "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceUpdatedEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .reference(branch1)
                .hashBefore(Hash.of("1234").asString())
                .hashAfter(Hash.of("5678").asString())
                .build());
  }

  @Test
  void newReferenceDeletedEvent() {
    EventFactory ef = new EventFactory(config);
    ReferenceDeletedResult result =
        ImmutableReferenceDeletedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("1234"))
            .build();
    Event actual = ef.newReferenceDeletedEvent(result, "repo1", () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableReferenceDeletedEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .reference(branch1)
                .hashBefore(Hash.of("1234").asString())
                .build());
  }

  @Test
  void newContentStoredEvent() {
    EventFactory ef = new EventFactory(config);
    Content table =
        ImmutableContent.builder()
            .id("table1")
            .type("ICEBERG_TABLE")
            .putProperty("metadataLocation", "location")
            .putProperty("schemaId", 1)
            .putProperty("specId", 2)
            .putProperty("sortOrderId", 3)
            .putProperty("snapshotId", 4)
            .build();
    Event actual =
        ef.newContentStoredEvent(
            BranchName.of("branch1"),
            Hash.of("1234"),
            ContentKey.of("foo.bar.table1"),
            table,
            "repo1",
            () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableContentStoredEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .reference(branch1)
                .hash(Hash.of("1234").asString())
                .contentKey(ContentKey.of("foo.bar.table1"))
                .content(table)
                .build());
  }

  @Test
  void newContentRemovedEvent() {
    EventFactory ef = new EventFactory(config);
    Event actual =
        ef.newContentRemovedEvent(
            BranchName.of("branch1"),
            Hash.of("1234"),
            ContentKey.of("foo.bar.table1"),
            "repo1",
            () -> "alice");
    assertThat(actual)
        .isEqualTo(
            ImmutableContentRemovedEvent.builder()
                .id(uuid)
                .repositoryId("repo1")
                .eventCreationTimestamp(now)
                .eventInitiator("alice")
                .putProperty("key", "value")
                .reference(branch1)
                .hash(Hash.of("1234").asString())
                .contentKey(ContentKey.of("foo.bar.table1"))
                .build());
  }
}
