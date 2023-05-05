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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
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
import org.slf4j.LoggerFactory;

@ExtendWith(MockitoExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestEventService {

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  EventConfig config;

  @Mock(answer = Answers.CALLS_REAL_METHODS)
  EventConfig.RetryConfig retryConfig;

  @Mock EventSubscribers subscribers;
  @Mock EventSubscriber subscriber1;
  @Mock EventSubscriber subscriber2;

  ListAppender<ILoggingEvent> listAppender;
  Level originalLevel;

  @BeforeEach
  void setUp() {
    when(config.getRetryConfig()).thenReturn(retryConfig);
    when(retryConfig.getInitialDelay()).thenReturn(Duration.ofMillis(10));
    when(subscribers.getSubscribers()).thenReturn(Arrays.asList(subscriber1, subscriber2));
    @SuppressWarnings("Slf4jIllegalPassedClass")
    Logger serviceLogger = (Logger) LoggerFactory.getLogger(EventService.class);
    originalLevel = serviceLogger.getLevel();
    serviceLogger.setLevel(Level.ERROR);
    listAppender = new ListAppender<>();
    listAppender.start();
    serviceLogger.addAppender(listAppender);
  }

  @AfterEach
  void tearDown() {
    @SuppressWarnings("Slf4jIllegalPassedClass")
    Logger serviceLogger = (Logger) LoggerFactory.getLogger(EventService.class);
    serviceLogger.detachAppender(listAppender);
    serviceLogger.setLevel(originalLevel);
    listAppender.stop();
  }

  @ParameterizedTest
  @MethodSource("allResults")
  void deliverySimple(Result result) throws Exception {
    // subscriber1 will accept the event
    // subscriber2 will reject the event
    when(subscriber1.accepts(any(Event.class))).thenReturn(true);
    when(subscriber2.accepts(any(Event.class))).thenReturn(false);
    switch (result.getResultType()) {
      case MERGE:
      case TRANSPLANT:
        when(subscribers.hasSubscribersFor(EventType.COMMIT)).thenReturn(true);
        // fall through
      case COMMIT:
        when(subscribers.hasSubscribersFor(EventType.CONTENT_STORED)).thenReturn(true);
        break;
      default:
    }
    EventFactory factory = new EventFactory(config);
    try (EventService eventService =
        new EventService(config, factory, subscribers, new DefaultEventExecutor())) {
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
          verify(subscriber1, times(3)).isBlocking(); // 3 events total
          verify(subscriber2, never()).isBlocking();
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber2, never()).onCommit(any());
          verify(subscriber2, never()).onContentStored(any());
          verify(subscriber2, never()).onContentRemoved(any());
          break;
        case MERGE:
          verify(subscriber1, times(4)).isBlocking(); // 4 events total
          verify(subscriber2, never()).isBlocking();
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
          verify(subscriber1, times(4)).isBlocking();
          verify(subscriber2, never()).isBlocking();
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
          verify(subscriber1).isBlocking();
          verify(subscriber2, never()).isBlocking();
          verify(subscriber1, timeout(5000)).onReferenceCreated(any());
          verify(subscriber2, never()).onReferenceCreated(any());
          break;
        case REFERENCE_ASSIGNED:
          verify(subscriber1).isBlocking();
          verify(subscriber2, never()).isBlocking();
          verify(subscriber1, timeout(5000)).onReferenceUpdated(any());
          verify(subscriber2, never()).onReferenceUpdated(any());
          break;
        case REFERENCE_DELETED:
          verify(subscriber1).isBlocking();
          verify(subscriber2, never()).isBlocking();
          verify(subscriber1, timeout(5000)).onReferenceDeleted(any());
          verify(subscriber2, never()).onReferenceDeleted(any());
          break;
        default:
      }
    }
    verify(subscriber1).close();
    verify(subscriber2).close();
    assertThat(listAppender.list).isEmpty();
    verifyNoMoreInteractions(subscriber1, subscriber2);
  }

  @ParameterizedTest
  @MethodSource("allResults")
  void deliveryWithRetries(Result result) throws Exception {
    when(subscriber1.accepts(any(Event.class))).thenReturn(true);
    when(subscriber2.accepts(any(Event.class))).thenReturn(true);
    when(subscriber1.isBlocking()).thenReturn(true);
    when(subscriber2.isBlocking()).thenReturn(false);
    // subscriber1 will fail twice, then succeed
    // subscriber2 will fail three times and give up
    switch (result.getResultType()) {
      case COMMIT:
        when(subscribers.hasSubscribersFor(EventType.CONTENT_STORED)).thenReturn(true);
        doAnswer(mockSubscriberFailures(2)).when(subscriber1).onCommit(any(CommitEvent.class));
        doAnswer(mockSubscriberFailures(3)).when(subscriber2).onCommit(any(CommitEvent.class));
        break;
      case MERGE:
        when(subscribers.hasSubscribersFor(EventType.COMMIT)).thenReturn(true);
        when(subscribers.hasSubscribersFor(EventType.CONTENT_STORED)).thenReturn(true);
        doAnswer(mockSubscriberFailures(2)).when(subscriber1).onMerge(any(MergeEvent.class));
        doAnswer(mockSubscriberFailures(3)).when(subscriber2).onMerge(any(MergeEvent.class));
        break;
      case TRANSPLANT:
        when(subscribers.hasSubscribersFor(EventType.COMMIT)).thenReturn(true);
        when(subscribers.hasSubscribersFor(EventType.CONTENT_STORED)).thenReturn(true);
        doAnswer(mockSubscriberFailures(2))
            .when(subscriber1)
            .onTransplant(any(TransplantEvent.class));
        doAnswer(mockSubscriberFailures(3))
            .when(subscriber2)
            .onTransplant(any(TransplantEvent.class));
        break;
      case REFERENCE_CREATED:
        doAnswer(mockSubscriberFailures(2))
            .when(subscriber1)
            .onReferenceCreated(any(ReferenceCreatedEvent.class));
        doAnswer(mockSubscriberFailures(3))
            .when(subscriber2)
            .onReferenceCreated(any(ReferenceCreatedEvent.class));
        break;
      case REFERENCE_ASSIGNED:
        doAnswer(mockSubscriberFailures(2))
            .when(subscriber1)
            .onReferenceUpdated(any(ReferenceUpdatedEvent.class));
        doAnswer(mockSubscriberFailures(3))
            .when(subscriber2)
            .onReferenceUpdated(any(ReferenceUpdatedEvent.class));
        break;
      case REFERENCE_DELETED:
        doAnswer(mockSubscriberFailures(2))
            .when(subscriber1)
            .onReferenceDeleted(any(ReferenceDeletedEvent.class));
        doAnswer(mockSubscriberFailures(3))
            .when(subscriber2)
            .onReferenceDeleted(any(ReferenceDeletedEvent.class));
        break;
      default:
    }
    EventFactory factory = new EventFactory(config);
    try (EventService eventService =
        new EventService(config, factory, subscribers, new DefaultEventExecutor())) {
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
          verify(subscriber1, times(3)).isBlocking(); // 3 events total
          verify(subscriber2, times(3)).isBlocking(); // 3 events total
          verify(subscriber1, timeout(5000).times(3)).onCommit(any());
          verify(subscriber2, timeout(5000).times(3)).onCommit(any());
          // delivery of derived events succeeded on first attempt
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber2, timeout(5000)).onContentStored(any());
          verify(subscriber2, timeout(5000)).onContentRemoved(any());
          break;
        case MERGE:
          verify(subscriber1, times(4)).isBlocking(); // 4 events total
          verify(subscriber2, times(4)).isBlocking(); // 4 events total
          verify(subscriber1, timeout(5000).times(3)).onMerge(any());
          verify(subscriber2, timeout(5000).times(3)).onMerge(any());
          // delivery of derived events succeeded on first attempt
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber2, timeout(5000)).onCommit(any());
          verify(subscriber2, timeout(5000)).onContentStored(any());
          verify(subscriber2, timeout(5000)).onContentRemoved(any());
          break;
        case TRANSPLANT:
          verify(subscriber1, times(4)).isBlocking(); // 4 events total
          verify(subscriber2, times(4)).isBlocking(); // 4 events total
          verify(subscriber1, timeout(5000).times(3)).onTransplant(any());
          verify(subscriber2, timeout(5000).times(3)).onTransplant(any());
          // delivery of derived events succeeded on first attempt
          verify(subscriber1, timeout(5000)).onCommit(any());
          verify(subscriber1, timeout(5000)).onContentRemoved(any());
          verify(subscriber1, timeout(5000)).onContentStored(any());
          verify(subscriber2, timeout(5000)).onCommit(any());
          verify(subscriber2, timeout(5000)).onContentStored(any());
          verify(subscriber2, timeout(5000)).onContentRemoved(any());
          break;
        case REFERENCE_CREATED:
          verify(subscriber1).isBlocking();
          verify(subscriber2).isBlocking();
          verify(subscriber1, timeout(5000).times(3)).onReferenceCreated(any());
          verify(subscriber2, timeout(5000).times(3)).onReferenceCreated(any());
          break;
        case REFERENCE_ASSIGNED:
          verify(subscriber1).isBlocking();
          verify(subscriber2).isBlocking();
          verify(subscriber1, timeout(5000).times(3)).onReferenceUpdated(any());
          verify(subscriber2, timeout(5000).times(3)).onReferenceUpdated(any());
          break;
        case REFERENCE_DELETED:
          verify(subscriber1).isBlocking();
          verify(subscriber2).isBlocking();
          verify(subscriber1, timeout(5000).times(3)).onReferenceDeleted(any());
          verify(subscriber2, timeout(5000).times(3)).onReferenceDeleted(any());
          break;
        default:
      }
    }
    verify(subscriber1).close();
    verify(subscriber2).close();
    verifyNoMoreInteractions(subscriber1, subscriber2);
    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () ->
                assertThat(listAppender.list)
                    .singleElement()
                    .satisfies(
                        log -> {
                          assertThat(log.getFormattedMessage())
                              .contains("could not be delivered on attempt 3, giving up");
                          assertThat(log.getThrowableProxy().getMessage())
                              .contains("failure3")
                              .doesNotContain("failure4");
                          assertThat(log.getThrowableProxy().getSuppressed())
                              .singleElement()
                              .satisfies(
                                  failure2 -> {
                                    assertThat(failure2.getMessage()).contains("failure2");
                                    assertThat(failure2.getSuppressed())
                                        .singleElement()
                                        .satisfies(
                                            failure1 ->
                                                assertThat(failure1.getMessage())
                                                    .contains("failure1"));
                                  });
                          assertThat(log.getMDCPropertyMap())
                              .containsKey(EventService.SUBSCRIPTION_ID_MDC_KEY);
                        }));
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

  private static Answer<Void> mockSubscriberFailures(int maxFailures) {
    AtomicInteger counter = new AtomicInteger(0);
    return invocation -> {
      if (counter.incrementAndGet() <= maxFailures) {
        throw new RuntimeException("failure" + counter);
      }
      return null;
    };
  }
}
