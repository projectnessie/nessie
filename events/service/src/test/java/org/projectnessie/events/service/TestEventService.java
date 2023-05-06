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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.Delete;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutableCommit;
import org.projectnessie.versioned.ImmutableCommitResult;
import org.projectnessie.versioned.ImmutableMergeResult;
import org.projectnessie.versioned.ImmutableReferenceAssignedResult;
import org.projectnessie.versioned.ImmutableReferenceCreatedResult;
import org.projectnessie.versioned.ImmutableReferenceDeletedResult;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.ResultType;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestEventService {

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  EventConfig config;

  @Mock EventSubscriber subscriber1;
  @Mock EventSubscriber subscriber2;

  @ParameterizedTest
  @MethodSource("allResults")
  void deliverySimple(Result result) throws Exception {
    // subscriber1 will accept the event
    // subscriber2 will reject the event
    when(subscriber1.accepts(any(EventType.class))).thenReturn(true);
    when(subscriber1.accepts(any(Event.class))).thenReturn(true);
    when(subscriber2.accepts(any(Event.class))).thenReturn(false);
    EventFactory factory = new EventFactory(config);
    EventSubscribers subscribers = new EventSubscribers(subscriber1, subscriber2);
    try (EventService eventService = new EventService(config, factory, subscribers)) {
      eventService.start();
      VersionStoreEvent versionStoreEvent =
          ImmutableVersionStoreEvent.builder()
              .result(result)
              .repositoryId("repo")
              .user(() -> "alice")
              .build();
      eventService.onVersionStoreEvent(versionStoreEvent);
      verify(subscriber1).onSubscribe(any());
      verify(subscriber2).onSubscribe(any());
      switch (result.getResultType()) {
        case COMMIT:
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber2, never()).onCommit(any());
          verify(subscriber2, never()).onContentStored(any());
          verify(subscriber2, never()).onContentRemoved(any());
          break;
        case MERGE:
          verify(subscriber1, timeout(5000)).onMerge(any());
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber2, never()).onMerge(any());
          verify(subscriber2, never()).onCommit(any());
          verify(subscriber2, never()).onContentStored(any());
          verify(subscriber2, never()).onContentRemoved(any());
          break;
        case TRANSPLANT:
          verify(subscriber1, timeout(5000)).onTransplant(any());
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber2, never()).onTransplant(any());
          verify(subscriber2, never()).onCommit(any());
          verify(subscriber2, never()).onContentStored(any());
          verify(subscriber2, never()).onContentRemoved(any());
          break;
        case REFERENCE_CREATED:
          verify(subscriber1, timeout(5000)).onReferenceCreated(any());
          verify(subscriber2, never()).onReferenceCreated(any());
          break;
        case REFERENCE_ASSIGNED:
          verify(subscriber1, timeout(5000)).onReferenceUpdated(any());
          verify(subscriber2, never()).onReferenceUpdated(any());
          break;
        case REFERENCE_DELETED:
          verify(subscriber1, timeout(5000)).onReferenceDeleted(any());
          verify(subscriber2, never()).onReferenceDeleted(any());
          break;
        default:
      }
    }
    verify(subscriber1).close();
    verify(subscriber2).close();
    verifyNoMoreInteractions(subscriber1, subscriber2);
  }

  private Stream<Result> allResults() {
    Commit commit =
        ImmutableCommit.builder()
            .hash(Hash.of("5678"))
            .parentHash(Hash.of("1234"))
            .commitMeta(
                org.projectnessie.model.ImmutableCommitMeta.builder()
                    .committer("committer")
                    .author("author")
                    .message("message")
                    .commitTime(Instant.now())
                    .authorTime(Instant.now())
                    .build())
            .operations(
                Arrays.asList(
                    Put.of(
                        ContentKey.of("foo.bar.table1"),
                        IcebergTable.of("somewhere", 42, 42, 42, 42, "table1")),
                    Delete.of(ContentKey.of("foo.bar.table2"))))
            .build();
    return Stream.of(
        ImmutableCommitResult.<Commit>builder()
            .commit(commit)
            .targetBranch(BranchName.of("branch1"))
            .build(),
        ImmutableMergeResult.<Commit>builder()
            .resultType(ResultType.MERGE)
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("1234")) // hash before
            .resultantTargetHash(Hash.of("5678")) // hash after
            .commonAncestor(Hash.of("0000"))
            .addCreatedCommits(commit)
            .build(),
        ImmutableMergeResult.<Commit>builder()
            .resultType(ResultType.TRANSPLANT)
            .sourceRef(BranchName.of("branch1"))
            .targetBranch(BranchName.of("branch2"))
            .effectiveTargetHash(Hash.of("1234")) // hash before
            .resultantTargetHash(Hash.of("5678")) // hash after
            .addCreatedCommits(commit)
            .build(),
        ImmutableReferenceCreatedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("1234"))
            .build(),
        ImmutableReferenceAssignedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .previousHash(Hash.of("1234"))
            .currentHash(Hash.of("5678"))
            .build(),
        ImmutableReferenceDeletedResult.builder()
            .namedRef(BranchName.of("branch1"))
            .hash(Hash.of("1234"))
            .build());
  }
}
