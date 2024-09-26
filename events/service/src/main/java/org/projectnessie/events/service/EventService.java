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

import jakarta.annotation.Nullable;
import java.security.Principal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.TransplantResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Base class for event services.
 *
 * <p>This class takes care of starting and stopping the subscribers, and provides helper methods
 * for handling incoming results from the version store, and for firing events.
 *
 * <p>This class is meant to be used as a singleton. It provides all the required functionality to
 * process and deliver events. Subclasses may override some of the protected methods to add support
 * for tracing, or to implement more sophisticated delivery logic.
 */
public class EventService implements AutoCloseable {

  public static final String SUBSCRIPTION_ID_MDC_KEY = "nessie.events.subscription.id";
  public static final String EVENT_ID_MDC_KEY = "nessie.events.event.id";

  private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

  protected final EventConfig config;
  protected final EventFactory factory;
  protected final EventSubscribers subscribers;

  private volatile boolean started;
  private boolean hasContentSubscribers;
  private boolean hasCommitSubscribers;

  public EventService(EventConfig config, EventFactory factory, EventSubscribers subscribers) {
    this.config = config;
    this.factory = factory;
    this.subscribers = subscribers;
  }

  /** Starts event delivery by activating the subscribers. */
  public synchronized void start() {
    if (!started) {
      subscribers.start(
          eventSubscriber ->
              ImmutableEventSubscription.builder()
                  .id(config.getIdGenerator().get())
                  .systemConfiguration(config.getSystemConfiguration())
                  .build());
      hasContentSubscribers =
          subscribers.hasSubscribersFor(EventType.CONTENT_STORED)
              || subscribers.hasSubscribersFor(EventType.CONTENT_REMOVED);
      hasCommitSubscribers =
          hasContentSubscribers || subscribers.hasSubscribersFor(EventType.COMMIT);
      started = true;
    }
  }

  /** Closes the event service by deactivating the subscribers. */
  @Override
  public synchronized void close() {
    subscribers.close();
  }

  /**
   * Invoked when a result is received from the version store by {@link ResultCollector}, then
   * forwarded to this service for delivery.
   *
   * @implSpec It is expected that this method is only invoked for results that are relevant to the
   *     subscribers of this service. For example, if this service is only interested in {@link
   *     ReferenceCreatedEvent}s, then it should only be invoked for {@link
   *     ReferenceCreatedResult}s. Similarly, if this service is only interested in {@link
   *     ContentStoredEvent}s, then it should be invoked for any result that might produce such
   *     events, namely: {@link CommitResult} and {@link MergeResult}.
   * @see ResultCollector#accept(Result)
   * @see ResultCollector#shouldProcess(Result)
   */
  public void onVersionStoreEvent(VersionStoreEvent event) {
    if (!started) {
      return;
    }
    Result result = event.getResult();
    Principal user = event.getUser().orElse(null);
    String repositoryId = event.getRepositoryId();
    switch (result.getResultType()) {
      case COMMIT:
        onCommitResult((CommitResult) result, repositoryId, user);
        break;
      case MERGE:
        onMergeResult((MergeResult) result, repositoryId, user);
        break;
      case TRANSPLANT:
        onTransplantResult((TransplantResult) result, repositoryId, user);
        break;
      case REFERENCE_CREATED:
        onReferenceCreatedResult((ReferenceCreatedResult) result, repositoryId, user);
        break;
      case REFERENCE_ASSIGNED:
        onReferenceAssignedResult((ReferenceAssignedResult) result, repositoryId, user);
        break;
      case REFERENCE_DELETED:
        onReferenceDeletedResult((ReferenceDeletedResult) result, repositoryId, user);
        break;
      default:
        throw new IllegalArgumentException("Unknown result type: " + result.getResultType());
    }
  }

  private void onCommitResult(CommitResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received commit result: {}", result);
    fireCommitEvent(result.getCommit(), result.getTargetBranch(), repositoryId, user);
  }

  private void onMergeResult(MergeResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received merge result: {}", result);
    fireMergeEvent(result, repositoryId, user);
  }

  private void onTransplantResult(
      TransplantResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received transplant result: {}", result);
    fireTransplantEvent(result, repositoryId, user);
  }

  private void onReferenceCreatedResult(
      ReferenceCreatedResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received branch created result: {}", result);
    fireEvent(factory.newReferenceCreatedEvent(result, repositoryId, user));
  }

  private void onReferenceAssignedResult(
      ReferenceAssignedResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received reference assigned result: {}", result);
    fireEvent(factory.newReferenceUpdatedEvent(result, repositoryId, user));
  }

  private void onReferenceDeletedResult(
      ReferenceDeletedResult result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received reference deleted result: {}", result);
    fireEvent(factory.newReferenceDeletedEvent(result, repositoryId, user));
  }

  private void fireCommitEvent(
      Commit commit, BranchName targetBranch, String repositoryId, @Nullable Principal user) {
    fireEvent(factory.newCommitEvent(commit, targetBranch, repositoryId, user));
    if (hasContentSubscribers) {
      fireContentEvents(commit, targetBranch, repositoryId, user);
    }
  }

  private void fireMergeEvent(MergeResult result, String repositoryId, @Nullable Principal user) {
    fireEvent(factory.newMergeEvent(result, repositoryId, user));
    if (hasCommitSubscribers) {
      for (Commit commit : result.getCreatedCommits()) {
        fireCommitEvent(commit, result.getTargetBranch(), repositoryId, user);
      }
    }
  }

  private void fireTransplantEvent(
      TransplantResult result, String repositoryId, @Nullable Principal user) {
    fireEvent(factory.newTransplantEvent(result, repositoryId, user));
    if (hasCommitSubscribers) {
      for (Commit commit : result.getCreatedCommits()) {
        fireCommitEvent(commit, result.getTargetBranch(), repositoryId, user);
      }
    }
  }

  private void fireContentEvents(
      Commit commit, BranchName targetBranch, String repositoryId, @Nullable Principal user) {
    List<Operation> operations = commit.getOperations();
    if (operations != null && !operations.isEmpty()) {
      Hash hash = Objects.requireNonNull(commit.getHash());
      Instant commitTime = Objects.requireNonNull(commit.getCommitMeta().getCommitTime());
      for (Operation operation : operations) {
        ContentKey contentKey = operation.getKey();
        if (operation instanceof Put) {
          Content content = ((Put) operation).getContent();
          fireEvent(
              factory.newContentStoredEvent(
                  targetBranch, hash, commitTime, contentKey, content, repositoryId, user));
        } else if (operation instanceof Delete) {
          fireEvent(
              factory.newContentRemovedEvent(
                  targetBranch, hash, commitTime, contentKey, repositoryId, user));
        }
      }
    }
  }

  /**
   * Forwards the event to all subscribers.
   *
   * @implNote This implementation is the simplest possible and just invokes all the subscribers one
   *     by one, synchronously and sequentially. Subclasses may override this method to implement a
   *     more sophisticated delivery mechanism, e.g. using an asynchronous event bus.
   */
  protected void fireEvent(Event event) {
    LOGGER.debug("Firing {} event: {}", event.getType(), event);
    for (Map.Entry<EventSubscription, EventSubscriber> entry :
        subscribers.getSubscriptions().entrySet()) {
      EventSubscription subscription = entry.getKey();
      EventSubscriber subscriber = entry.getValue();
      deliverEvent(event, subscriber, subscription);
    }
  }

  protected void deliverEvent(
      Event event, EventSubscriber subscriber, EventSubscription subscription) {
    MDC.put(SUBSCRIPTION_ID_MDC_KEY, subscription.getIdAsText());
    MDC.put(EVENT_ID_MDC_KEY, event.getIdAsText());
    try {
      if (subscriber.accepts(event)) {
        LOGGER.debug("Delivering event to subscriber {}: {}", subscriber, event);
        subscriber.onEvent(event);
        LOGGER.debug("Event successfully delivered: {}", event);
      } else {
        LOGGER.debug("Subscriber rejected event: {}", event);
      }
    } catch (Exception e) {
      LOGGER.error("Event could not be delivered: {}", event, e);
    } finally {
      MDC.remove(SUBSCRIPTION_ID_MDC_KEY);
      MDC.remove(EVENT_ID_MDC_KEY);
    }
  }
}
