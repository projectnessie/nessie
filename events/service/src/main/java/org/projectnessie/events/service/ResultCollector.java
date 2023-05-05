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

import java.security.Principal;
import java.util.function.Consumer;
import org.projectnessie.versioned.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collector for {@link Result}s produced by the version store.
 *
 * <p>Instances of this class are meant to be injected into version stores to collect results.
 *
 * <p>The main reason why this functionality is not part of the {@link EventService} is that {@link
 * EventService} is meant to be a singleton, while {@link ResultCollector} is meant to be
 * instantiated per request, in order to capture the user principal that initiated the request, and
 * the target repository id.
 */
public class ResultCollector implements Consumer<Result> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ResultCollector.class);

  protected final EventSubscribers subscribers;
  protected final String repositoryId;
  protected final Principal user;
  protected final Consumer<VersionStoreEvent> destination;

  /**
   * Creates a new instance that forwards received {@link Result}s to the given {@link EventService}
   * synchronously.
   */
  public ResultCollector(
      EventSubscribers subscribers, String repositoryId, Principal user, EventService destination) {
    this(subscribers, repositoryId, user, destination::onVersionStoreEvent);
  }

  /**
   * Creates a new instance that forwards received {@link Result}s to the given {@link Consumer},
   * allowing implementers to control how the event is delivered to the {@link EventService}.
   */
  public ResultCollector(
      EventSubscribers subscribers,
      String repositoryId,
      Principal user,
      Consumer<VersionStoreEvent> destination) {
    this.subscribers = subscribers;
    this.repositoryId = repositoryId;
    this.user = user;
    this.destination = destination;
  }

  /** Called when a result is produced by the version store. */
  @Override
  public void accept(Result result) {
    if (shouldProcess(result)) {
      LOGGER.debug("Processing received result: {}", result);
      VersionStoreEvent event =
          ImmutableVersionStoreEvent.builder()
              .result(result)
              .repositoryId(repositoryId)
              .user(user)
              .build();
      forwardToEventService(event);
    } else {
      LOGGER.debug("Ignoring received result: {}", result);
    }
  }

  /**
   * Returns {@code true} if the given {@link Result} should be processed.
   *
   * <p>This acts as an early filter to remove results that are not relevant to any of the
   * subscribers.
   */
  protected boolean shouldProcess(Result result) {
    return subscribers.hasSubscribersFor(result.getResultType());
  }

  /**
   * Forwards the received {@link VersionStoreEvent} to the destination for delivery to subscribers.
   *
   * @implSpec The simplest implementation possible is to just call {@link
   *     EventService#onVersionStoreEvent(VersionStoreEvent)}. This is what this method does when
   *     the collector is created with the {@link ResultCollector#ResultCollector(EventSubscribers,
   *     String, Principal, EventService)} constructor. But implementers must keep in mind that such
   *     a synchronous processing might not be ideal, since the caller thread is one of Nessie's
   *     REST API threads.
   */
  protected void forwardToEventService(VersionStoreEvent event) {
    destination.accept(event);
  }
}
