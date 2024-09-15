/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.events.service.catalog;

import java.security.Principal;
import java.util.function.Consumer;
import org.projectnessie.catalog.model.ops.CatalogOperationResult;
import org.projectnessie.events.service.EventService;
import org.projectnessie.events.service.EventSubscribers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collector for {@link CatalogOperationResult}s produced by the catalog.
 *
 * <p>Instances of this class are meant to be injected into Catalog services.
 *
 * <p>The main reason why this functionality is not part of the {@link EventService} is that {@link
 * EventService} is meant to be a singleton, while {@link CatalogOperationResultCollector} is meant
 * to be instantiated per request, in order to capture the user principal that initiated the
 * request, and the target repository id.
 */
public class CatalogOperationResultCollector implements Consumer<CatalogOperationResult> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(CatalogOperationResultCollector.class);

  protected final EventSubscribers subscribers;
  protected final String repositoryId;
  protected final Principal user;
  protected final Consumer<CatalogOperationResultEvent> destination;

  /**
   * Creates a new instance that forwards received {@link CatalogOperationResult}s to the given
   * {@link EventService} synchronously.
   */
  public CatalogOperationResultCollector(
      EventSubscribers subscribers, String repositoryId, Principal user, EventService destination) {
    this(subscribers, repositoryId, user, destination::onCatalogEvent);
  }

  /**
   * Creates a new instance that forwards received {@link CatalogOperationResult}s to the given
   * {@link Consumer}, allowing implementers to control how the event is delivered to the {@link
   * EventService}.
   */
  public CatalogOperationResultCollector(
      EventSubscribers subscribers,
      String repositoryId,
      Principal user,
      Consumer<CatalogOperationResultEvent> destination) {
    this.subscribers = subscribers;
    this.repositoryId = repositoryId;
    this.user = user;
    this.destination = destination;
  }

  /** Called when an update to the catalog happens. */
  @Override
  public void accept(CatalogOperationResult result) {
    if (shouldProcess(result)) {
      LOGGER.debug("Processing received catalog commit: {}", result);
      CatalogOperationResultEvent event =
          ImmutableCatalogOperationResultEvent.builder()
              .repositoryId(repositoryId)
              .user(user)
              .catalogOperationResult(result)
              .build();
      forwardToEventService(event);
    } else {
      LOGGER.debug("Ignoring received catalog commit: {}", result);
    }
  }

  protected boolean shouldProcess(CatalogOperationResult result) {
    return subscribers.hasSubscribersFor(result.getOperation().getContentType());
  }

  /**
   * Forwards the received {@link CatalogOperationResultEvent} to the destination for delivery to
   * subscribers.
   *
   * @implSpec The simplest implementation possible is to just call {@link
   *     EventService#onCatalogEvent(CatalogOperationResultEvent)}. This is what this method does
   *     when the collector is created with the {@link
   *     #CatalogOperationResultCollector(EventSubscribers, String, Principal, EventService)}
   *     constructor. But implementers must keep in mind that such a synchronous processing might
   *     not be ideal, since the caller thread is one of Nessie's Catalog REST API threads.
   */
  protected void forwardToEventService(CatalogOperationResultEvent event) {
    destination.accept(event);
  }
}
