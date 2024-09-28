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
package org.projectnessie.operator.events;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.EventBuilder;
import io.fabric8.kubernetes.api.model.EventList;
import io.fabric8.kubernetes.api.model.EventSource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.MicroTime;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ObjectReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.AggregatedOperatorException;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.net.HttpURLConnection;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.projectnessie.operator.exception.NessieOperatorException;
import org.projectnessie.operator.reconciler.nessie.NessieReconciler;
import org.projectnessie.operator.reconciler.nessie.resource.Nessie;
import org.projectnessie.operator.reconciler.nessiegc.NessieGcReconciler;
import org.projectnessie.operator.reconciler.nessiegc.resource.NessieGc;
import org.projectnessie.operator.utils.EventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to manage events.
 *
 * <p>Events are unique for each combination of primary resource and reason. The event is updated
 * when an event with the same reason is fired again for the same resource.
 *
 * <p>Loosely inspired from <a
 * href="https://github.com/kubernetes/client-go/blob/master/tools/events/event_broadcaster.go">event_broadcaster.go</a>.
 */
@ApplicationScoped
public class EventService {

  private static final String CONTEXT_KEY = "event-service";

  public static EventService retrieveFromContext(Context<?> context) {
    return context.managedDependentResourceContext().getMandatory(CONTEXT_KEY, EventService.class);
  }

  public static void storeInContext(Context<?> context, EventService eventService) {
    context.managedDependentResourceContext().put(CONTEXT_KEY, eventService);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EventService.class);

  private final ConcurrentMap<String, Map<EventReason, Event>> eventsCache =
      new ConcurrentHashMap<>();

  private final KubernetesClient client;

  @Inject
  public EventService(KubernetesClient client) {
    this.client = client;
  }

  public void fireEvent(HasMetadata primary, EventReason reason, String message, Object... args) {
    eventsCache
        .computeIfAbsent(primary.getMetadata().getUid(), uid -> loadEvents(primary))
        .compute(reason, (r, ev) -> createOrUpdateEvent(1, primary, r, ev, message, args));
  }

  public void fireErrorEvent(HasMetadata primary, Throwable t) {
    t = EventUtils.launderThrowable(t, AggregatedOperatorException.class);
    if (t instanceof AggregatedOperatorException aoe) {
      aoe.getAggregatedExceptions().values().stream()
          .map(e -> EventUtils.launderThrowable(e, NessieOperatorException.class))
          .forEach(
              error ->
                  fireEvent(
                      primary, EventUtils.errorReason(error), EventUtils.getErrorMessage(error)));
    } else {
      t = EventUtils.launderThrowable(t, NessieOperatorException.class);
      fireEvent(primary, EventUtils.errorReason(t), EventUtils.getErrorMessage(t));
    }
  }

  private Event createOrUpdateEvent(
      int attempt,
      HasMetadata primary,
      EventReason reason,
      Event current,
      String message,
      Object... args) {
    try {
      ZonedDateTime now = ZonedDateTime.now();
      String timestamp = EventUtils.formatTime(now);
      MicroTime microTime = new MicroTime(EventUtils.formatMicroTime(now));
      String formatted = EventUtils.formatMessage(message, args);
      Event updated =
          current == null
              ? newEvent(primary, reason, formatted, timestamp, microTime)
              : editEvent(current, formatted, timestamp, microTime);
      Resource<Event> resource = client.v1().events().resource(updated);
      // Note: server-side apply would be a good option, but it's not compatible with unit tests
      return current == null ? resource.create() : resource.update();
    } catch (Exception e) {
      // We are the only ones updating these events, but conflicts can happen when
      // bouncing the operator pod or reinstalling the operator, since there could
      // be more than one operator instance alive for a short period of time.
      if (e instanceof KubernetesClientException kce
          && kce.getCode() == HttpURLConnection.HTTP_CONFLICT
          && attempt < 3) {
        LOGGER.debug("Event was updated concurrently, retrying");
        current =
            client
                .v1()
                .events()
                .inNamespace(primary.getMetadata().getNamespace())
                .withName(EventUtils.eventName(primary, reason))
                .require();
        return createOrUpdateEvent(attempt + 1, primary, reason, current, message, args);
      }
      LOGGER.warn("Failed to create or update event", e);
      return current;
    }
  }

  private ConcurrentMap<EventReason, Event> loadEvents(HasMetadata primary) {
    ConcurrentMap<EventReason, Event> events = new ConcurrentHashMap<>();
    try {
      for (Event event : eventsFor(primary).list().getItems()) {
        EventReason reason = EventUtils.reasonFromEventName(event.getMetadata().getName());
        events.put(reason, event);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to load events", e);
    }
    if (!events.isEmpty()) {
      LOGGER.info("Loaded {} events", events.size());
    }
    return events;
  }

  public void clearEvents(HasMetadata primary) {
    LOGGER.debug("Deleting events");
    eventsCache.remove(primary.getMetadata().getUid());
    try {
      eventsFor(primary).delete();
    } catch (Exception e) {
      LOGGER.warn("Failed to delete events", e);
    }
  }

  private FilterWatchListDeletable<Event, EventList, Resource<Event>> eventsFor(
      HasMetadata primary) {
    return client
        .v1()
        .events()
        .inNamespace(primary.getMetadata().getNamespace())
        .withInvolvedObject(
            new ObjectReferenceBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .withUid(primary.getMetadata().getUid())
                .build());
  }

  private Event newEvent(
      HasMetadata primary,
      EventReason reason,
      String formatted,
      String timestamp,
      MicroTime microTime) {
    String eventName = EventUtils.eventName(primary, reason);
    LOGGER.debug("Creating event {}", eventName);
    return new EventBuilder()
        .withMetadata(
            new ObjectMetaBuilder()
                .withName(eventName)
                .withNamespace(primary.getMetadata().getNamespace())
                .build())
        .withEventTime(microTime)
        .withType(reason.type().name())
        .withReason(reason.name())
        .withMessage(formatted)
        .withAction("Reconcile")
        .withCount(1)
        .withFirstTimestamp(timestamp)
        .withLastTimestamp(timestamp)
        .withInvolvedObject(
            new ObjectReferenceBuilder()
                .withName(primary.getMetadata().getName())
                .withNamespace(primary.getMetadata().getNamespace())
                .withUid(primary.getMetadata().getUid())
                .withResourceVersion(primary.getMetadata().getResourceVersion())
                .withApiVersion(primary.getApiVersion())
                .withKind(primary.getKind())
                .build())
        .withSource(new EventSource(getComponent(primary), null))
        .withReportingComponent(getComponent(primary))
        // TODO add complete pod name
        .withReportingInstance("nessie-operator")
        .build();
  }

  private Event editEvent(Event current, String formatted, String timestamp, MicroTime microTime) {
    EventBuilder eventBuilder =
        new EventBuilder(current)
            .editMetadata()
            .withManagedFields(Collections.emptyList())
            .endMetadata()
            .withMessage(formatted)
            .withLastTimestamp(timestamp)
            .editOrNewSeries()
            .withLastObservedTime(microTime)
            .endSeries();
    // Only update the count if the message has changed, otherwise
    // updating the last observed time only is enough
    if (!formatted.equals(current.getMessage())) {
      int count = current.getCount() == null ? 1 : current.getCount();
      count++;
      eventBuilder.withCount(count).editOrNewSeries().withCount(count).endSeries();
    }
    Event event = eventBuilder.build();
    LOGGER.debug(
        "Updating event {}, new count = {}", current.getMetadata().getName(), event.getCount());
    return event;
  }

  private static String getComponent(HasMetadata primary) {
    return switch (primary.getKind()) {
      case Nessie.KIND -> NessieReconciler.NAME;
      case NessieGc.KIND -> NessieGcReconciler.NAME;
      default -> throw new IllegalArgumentException("Unknown kind " + primary.getKind());
    };
  }
}
