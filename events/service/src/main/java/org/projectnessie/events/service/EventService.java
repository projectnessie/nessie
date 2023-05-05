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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.service.util.ContentMapping;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;
import org.projectnessie.events.spi.ImmutableEventSubscription;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Commit;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.MergeResult;
import org.projectnessie.versioned.Operation;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.ReferenceAssignedResult;
import org.projectnessie.versioned.ReferenceCreatedResult;
import org.projectnessie.versioned.ReferenceDeletedResult;
import org.projectnessie.versioned.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Base class for event services.
 *
 * <p>This class takes care of starting and stopping the subscribers, and provides helper methods
 * for handling incoming results from the version store, and for firing events.
 *
 * <p>It also provides a {@link EventSubscriberHandler} inner class that should be used to implement
 * the subscriber-specific delivery logic, including retries and backoff.
 *
 * <p>This class is meant to be used as a singleton. It provides all the required functionality to
 * process and deliver events. Subclasses may override some of the protected methods to add support
 * for tracing, or to implement more sophisticated delivery logic.
 */
public class EventService implements AutoCloseable {

  public static final String SUBSCRIPTION_ID_MDC_KEY = "nessie.events.subscription.id";

  private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

  protected final EventConfig config;
  protected final EventFactory factory;
  protected final EventSubscribers subscribers;
  protected final EventExecutor executor;
  private final List<EventSubscriberHandler<?>> handlers = new CopyOnWriteArrayList<>();

  // guarded by this
  private boolean started;
  // guarded by this
  private boolean closed;

  public EventService(
      EventConfig config,
      EventFactory factory,
      EventSubscribers subscribers,
      EventExecutor executor) {
    this.config = config;
    this.factory = factory;
    this.subscribers = subscribers;
    this.executor = executor;
  }

  /** Starts event delivery by activating the subscriber handlers. */
  public synchronized void start() {
    if (!started) {
      LOGGER.info("Starting subscribers...");
      for (EventSubscriber subscriber : subscribers.getSubscribers()) {
        try {
          EventSubscription subscription =
              ImmutableEventSubscription.builder()
                  .id(config.getIdGenerator().get())
                  .systemConfiguration(config.getSystemConfiguration())
                  .build();
          EventSubscriberHandler<?> handler = createSubscriberHandler(subscriber, subscription);
          handler.start();
          handlers.add(handler);
        } catch (Exception e) {
          LOGGER.error("Error starting subscriber", e);
        }
      }
      LOGGER.info("Done starting subscribers.");
      started = true;
    }
  }

  /** Closes the event service by deactivating the subscriber handlers. */
  @Override
  public synchronized void close() {
    if (!closed) {
      LOGGER.info("Closing subscribers...");
      for (EventSubscriberHandler<?> handler : handlers) {
        try {
          handler.close();
        } catch (Exception e) {
          LOGGER.error("Error closing subscriber", e);
        }
      }
      LOGGER.info("Done closing subscribers.");
      handlers.clear();
      closed = true;
    }
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
  @SuppressWarnings("unchecked")
  public void onVersionStoreEvent(VersionStoreEvent event) {
    Result result = event.getResult();
    Principal user = event.getUser().orElse(null);
    String repositoryId = event.getRepositoryId();
    switch (result.getResultType()) {
      case COMMIT:
        onCommitResult((CommitResult<Commit>) result, repositoryId, user);
        break;
      case MERGE:
        onMergeResult((MergeResult<Commit>) result, repositoryId, user);
        break;
      case TRANSPLANT:
        onTransplantResult((MergeResult<Commit>) result, repositoryId, user);
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

  private void onCommitResult(
      CommitResult<Commit> result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received commit result: {}", result);
    fireCommitEvent(result.getCommit(), result.getTargetBranch(), repositoryId, user);
  }

  private void onMergeResult(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    LOGGER.debug("Received merge result: {}", result);
    fireMergeEvent(result, repositoryId, user);
  }

  private void onTransplantResult(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
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
    if (hasContentSubscribers()) {
      fireContentEvents(commit, targetBranch, repositoryId, user);
    }
  }

  private void fireMergeEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    fireEvent(factory.newMergeEvent(result, repositoryId, user));
    if (hasCommitSubscribers()) {
      for (Commit commit : result.getCreatedCommits()) {
        fireCommitEvent(commit, result.getTargetBranch(), repositoryId, user);
      }
    }
  }

  private void fireTransplantEvent(
      MergeResult<Commit> result, String repositoryId, @Nullable Principal user) {
    fireEvent(factory.newTransplantEvent(result, repositoryId, user));
    if (hasCommitSubscribers()) {
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
      for (org.projectnessie.versioned.Operation operation : operations) {
        if (operation instanceof org.projectnessie.versioned.Put) {
          ContentKey contentKey = ContentMapping.map(operation.getKey());
          Content content = ContentMapping.map(((Put) operation).getValue());
          fireEvent(
              factory.newContentStoredEvent(
                  targetBranch, hash, contentKey, content, repositoryId, user));
        } else if (operation instanceof org.projectnessie.versioned.Delete) {
          ContentKey contentKey = ContentMapping.map(operation.getKey());
          fireEvent(
              factory.newContentRemovedEvent(targetBranch, hash, contentKey, repositoryId, user));
        }
      }
    }
  }

  private void fireEvent(Event event) {
    LOGGER.debug("Firing {} event: {}", event.getType(), event);
    forwardToSubscribers(event);
  }

  private boolean hasContentSubscribers() {
    return subscribers.hasSubscribersFor(EventType.CONTENT_STORED)
        || subscribers.hasSubscribersFor(EventType.CONTENT_REMOVED);
  }

  private boolean hasCommitSubscribers() {
    return subscribers.hasSubscribersFor(EventType.COMMIT) || hasContentSubscribers();
  }

  /**
   * Forwards the event to all subscribers.
   *
   * @implNote This implementation is the simplest possible and just invokes all the subscriber
   *     handlers one by one, synchronously and sequentially. Subclasses may override this method to
   *     implement a more sophisticated delivery mechanism, e.g. using an asynchronous event bus.
   */
  protected void forwardToSubscribers(Event event) {
    for (EventSubscriberHandler<?> handler : handlers) {
      handler.deliver(event);
    }
  }

  /**
   * Creates an {@link EventSubscriberHandler} for the given subscriber.
   *
   * @implNote This implementation simply creates a new, vanilla, context-less {@link
   *     EventSubscriberHandler} for the given subscriber. Subclasses may override this method to
   *     provide custom implementations, e.g. with a tracing context.
   */
  protected EventSubscriberHandler<?> createSubscriberHandler(
      EventSubscriber subscriber, EventSubscription subscription) {
    return new EventSubscriberHandler<>(subscriber, subscription);
  }

  /**
   * A handler for a single subscriber.
   *
   * <p>Handlers are responsible for delivering events to the subscriber and retrying failed
   * delivery attempts.
   *
   * <p>Handlers also offer the possibility to pass an arbitrary context object from the caller
   * thread to the threads doing the retry attempts. This is useful e.g. for propagating traces.
   *
   * @param <CTX> The context type for the subscriber.
   */
  public class EventSubscriberHandler<CTX> {

    protected final EventSubscriber subscriber;
    protected final EventSubscription subscription;
    private final String subscriptionId;
    private volatile CompletionStage<Void> retryFuture;

    protected EventSubscriberHandler(EventSubscriber subscriber, EventSubscription subscription) {
      this.subscriber = subscriber;
      this.subscription = subscription;
      subscriptionId = subscription.getId().toString();
    }

    protected void start() {
      this.subscriber.onSubscribe(subscription);
    }

    protected void deliver(Event event) {
      try {
        MDC.put(SUBSCRIPTION_ID_MDC_KEY, subscriptionId);
        if (subscriber.accepts(event)) {
          LOGGER.debug("Received event: {}", event);
          CTX context = currentContext();
          Duration initialDelay = config.getRetryConfig().getInitialDelay();
          if (subscriber.isBlocking()) {
            deliverAsync(event, 1, initialDelay, null, context);
          } else {
            deliverSync(event, 1, initialDelay, null, context);
          }
        } else {
          LOGGER.debug("Subscriber rejected event: {}", event);
        }
      } finally {
        MDC.remove(SUBSCRIPTION_ID_MDC_KEY);
      }
    }

    /**
     * Delivers the event to the subscriber asynchronously.
     *
     * <p>This method is called by {@link #deliver(Event)} if the subscriber is blocking. The actual
     * delivery is done in a separate thread that is allowed to block.
     */
    protected void deliverAsync(
        Event event,
        int deliveryAttempt,
        Duration delay,
        Throwable previousError,
        @Nullable CTX context) {
      executor
          .submitBlocking(() -> doDelivery(event, context))
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  handleDeliveryError(
                      event,
                      deliveryAttempt,
                      delay,
                      maybeAddSuppressed(error, previousError),
                      context,
                      this::deliverAsync);
                }
              });
    }

    /**
     * Delivers the event to the subscriber synchronously, that is, on the caller's thread. Error
     * handling and retry attempt scheduling are also done in the caller's thread.
     */
    protected void deliverSync(
        Event event,
        int deliveryAttempt,
        Duration delay,
        Throwable previousError,
        @Nullable CTX context) {
      try {
        doDelivery(event, context);
      } catch (Exception e) {
        handleDeliveryError(
            event,
            deliveryAttempt,
            delay,
            maybeAddSuppressed(e, previousError),
            context,
            this::deliverSync);
      }
    }

    protected void doDelivery(Event event, @SuppressWarnings("unused") @Nullable CTX context) {
      try {
        MDC.put(SUBSCRIPTION_ID_MDC_KEY, subscriptionId);
        LOGGER.debug("Delivering event to subscriber {}: {}", subscriber, event);
        invokeSubscriberMethod(event);
        LOGGER.debug("Event successfully delivered: {}", event);
      } finally {
        MDC.remove(SUBSCRIPTION_ID_MDC_KEY);
      }
    }

    protected void handleDeliveryError(
        Event event,
        int currentAttempt,
        Duration currentDelay,
        Throwable error,
        CTX context,
        DeliveryWithRetry<CTX> invocation) {
      try {
        MDC.put(SUBSCRIPTION_ID_MDC_KEY, subscriptionId);
        if (currentAttempt < config.getRetryConfig().getMaxRetries()) {
          LOGGER.debug(
              "Event {} could not be delivered on attempt {}, retrying in {} ms",
              event,
              currentAttempt,
              currentDelay);
          retryFuture =
              executor.scheduleRetry(
                  () -> {
                    int nextAttempt = currentAttempt + 1;
                    Duration nextDelay = config.getRetryConfig().getNextDelay(currentDelay);
                    invocation.tryDeliver(event, nextAttempt, nextDelay, error, context);
                  },
                  currentDelay);
        } else {
          LOGGER.error(
              "Event {} could not be delivered on attempt {}, giving up",
              event,
              currentAttempt,
              error);
        }
      } finally {
        MDC.remove(SUBSCRIPTION_ID_MDC_KEY);
      }
    }

    /**
     * Returns the current context object.
     *
     * <p>The context will be retrieved on the call to {@link #deliver(Event)} and propagated to all
     * retry attempts. This can be used to propagate tracing spans across multiple attempts.
     *
     * <p>It is perfectly fine to return {@code null} if no context is required.
     */
    @Nullable
    protected CTX currentContext() {
      return null;
    }

    private void invokeSubscriberMethod(Event event) {
      switch (event.getType()) {
        case COMMIT:
          subscriber.onCommit((CommitEvent) event);
          break;
        case MERGE:
          subscriber.onMerge((MergeEvent) event);
          break;
        case TRANSPLANT:
          subscriber.onTransplant((TransplantEvent) event);
          break;
        case REFERENCE_CREATED:
          subscriber.onReferenceCreated((ReferenceCreatedEvent) event);
          break;
        case REFERENCE_UPDATED:
          subscriber.onReferenceUpdated((ReferenceUpdatedEvent) event);
          break;
        case REFERENCE_DELETED:
          subscriber.onReferenceDeleted((ReferenceDeletedEvent) event);
          break;
        case CONTENT_STORED:
          subscriber.onContentStored((ContentStoredEvent) event);
          break;
        case CONTENT_REMOVED:
          subscriber.onContentRemoved((ContentRemovedEvent) event);
          break;
        default:
          throw new IllegalArgumentException("Unknown event type: " + event.getType());
      }
    }

    protected void close() throws Exception {
      CompletionStage<Void> future = retryFuture;
      if (future != null) {
        future.toCompletableFuture().cancel(true);
      }
      retryFuture = null;
      subscriber.close();
    }
  }

  @FunctionalInterface
  private interface DeliveryWithRetry<CTX> {

    void tryDeliver(
        Event event, int deliveryAttempt, Duration delay, Throwable previousError, CTX context);
  }

  private static Throwable maybeAddSuppressed(Throwable cause, Throwable suppressed) {
    if (suppressed != null) {
      cause.addSuppressed(suppressed);
    }
    return cause;
  }
}
