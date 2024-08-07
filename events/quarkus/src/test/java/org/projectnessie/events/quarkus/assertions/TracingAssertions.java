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
package org.projectnessie.events.quarkus.assertions;

import static io.opentelemetry.sdk.trace.data.StatusData.error;
import static io.opentelemetry.sdk.trace.data.StatusData.ok;
import static io.opentelemetry.sdk.trace.data.StatusData.unset;
import static io.opentelemetry.semconv.ExceptionAttributes.EXCEPTION_TYPE;
import static io.opentelemetry.semconv.ServiceAttributes.SERVICE_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.projectnessie.events.api.EventType.COMMIT;
import static org.projectnessie.events.api.EventType.CONTENT_REMOVED;
import static org.projectnessie.events.api.EventType.CONTENT_STORED;
import static org.projectnessie.events.api.EventType.MERGE;
import static org.projectnessie.events.api.EventType.REFERENCE_CREATED;
import static org.projectnessie.events.api.EventType.REFERENCE_DELETED;
import static org.projectnessie.events.api.EventType.TRANSPLANT;
import static org.projectnessie.events.quarkus.assertions.EventAssertions.TIMEOUT;
import static org.projectnessie.events.quarkus.collector.QuarkusTracingResultCollector.NESSIE_RESULT_TYPE_ATTRIBUTE_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.DELIVERY_ATTEMPT_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.EVENT_ID_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.EVENT_TYPE_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.RETRIES_KEY;
import static org.projectnessie.events.quarkus.delivery.TracingEventDelivery.SUBSCRIPTION_ID_KEY;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.data.StatusData;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ObjectAssert;
import org.projectnessie.events.quarkus.fixtures.MockEventSubscriber;
import org.projectnessie.events.quarkus.fixtures.MockSpanExporter;
import org.projectnessie.versioned.ResultType;

@Singleton
public class TracingAssertions {
  public static final AttributeKey<String> ENDUSER_ID = AttributeKey.stringKey("enduser.id");

  @Inject Instance<MockSpanExporter> exporter;
  @Inject Instance<Tracer> tracer;

  @Inject MockEventSubscriber.MockEventSubscriber1 subscriber1;

  @Inject MockEventSubscriber.MockEventSubscriber2 subscriber2;

  public void reset() {
    if (exporter.isResolvable()) {
      exporter.get().reset();
    }
  }

  public void assertOpenTelemetryDisabled() {
    assertThat(tracer.isResolvable()).isFalse();
  }

  // Note: the assertions below rely on the predefined subscriber behavior, see MockEventSubscriber
  // for details

  public void awaitAndAssertCommitTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertCommitTraces(authEnabled));
  }

  public void awaitAndAssertMergeTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertMergeTraces(authEnabled));
  }

  public void awaitAndAssertTransplantTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertTransplantTraces(authEnabled));
  }

  public void awaitAndAssertReferenceCreatedTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceCreatedTraces(authEnabled));
  }

  public void awaitAndAssertReferenceDeletedTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceDeletedTraces(authEnabled));
  }

  public void awaitAndAssertReferenceUpdatedTraces(boolean authEnabled) {
    await().atMost(TIMEOUT).untilAsserted(() -> assertReferenceUpdatedTraces(authEnabled));
  }

  private void assertCommitTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .containsExactlyInAnyOrder(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.COMMIT publish",
            "nessie.events.subscribers.COMMIT receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_STORED publish",
            "nessie.events.subscribers.CONTENT_STORED receive",
            "nessie.events.subscribers.CONTENT_STORED receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_REMOVED publish",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            // delivery to subscriber 1
            "nessie.events.subscribers.COMMIT delivery",
            "nessie.events.subscribers.COMMIT attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_STORED delivery",
            "nessie.events.subscribers.CONTENT_STORED attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1",
            // delivery to subscriber 2 (rejected)
            "nessie.events.subscribers.CONTENT_STORED delivery",
            // delivery to subscriber 2 (2 failures, then success)
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 2", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 3");
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.COMMIT);
    assertSpanStatusEquals("nessie.results", unset());
    assertSpanStatusEquals("delivery", ok());
    assertSpanStatusEquals("(COMMIT|CONTENT_STORED) attempt", ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber1, ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber2, error());
    assertSpanStatusEquals("CONTENT_REMOVED attempt 3", ok());
    assertSpanException("CONTENT_REMOVED attempt (1|2)");
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("COMMIT", EVENT_TYPE_KEY, COMMIT.name());
    assertSpanAttributeEquals("CONTENT_STORED", EVENT_TYPE_KEY, CONTENT_STORED.name());
    assertSpanAttributeEquals("CONTENT_REMOVED", EVENT_TYPE_KEY, CONTENT_REMOVED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanAttributeEquals("attempt 2", DELIVERY_ATTEMPT_KEY, 2L);
    assertSpanAttributeEquals("CONTENT_REMOVED delivery", RETRIES_KEY, 2L);
    assertSpanEventPresent("(COMMIT|CONTENT_REMOVED) delivery", "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber1, "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber2, "delivery rejected");
  }

  private void assertMergeTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .containsExactlyInAnyOrder(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.MERGE publish",
            "nessie.events.subscribers.MERGE receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.COMMIT publish",
            "nessie.events.subscribers.COMMIT receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_STORED publish",
            "nessie.events.subscribers.CONTENT_STORED receive",
            "nessie.events.subscribers.CONTENT_STORED receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_REMOVED publish",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            // delivery to subscriber 1
            "nessie.events.subscribers.MERGE delivery",
            "nessie.events.subscribers.MERGE attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.COMMIT delivery",
            "nessie.events.subscribers.COMMIT attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_STORED delivery",
            "nessie.events.subscribers.CONTENT_STORED attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1",
            // delivery to subscriber 2 (rejected)
            "nessie.events.subscribers.CONTENT_STORED delivery",
            // delivery to subscriber 2 (2 failures, then success)
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 2", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 3");
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.MERGE);
    assertSpanStatusEquals("nessie.results", unset());
    assertSpanStatusEquals("delivery", ok());
    assertSpanStatusEquals("(MERGE|COMMIT|CONTENT_STORED) attempt", ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber1, ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber2, error());
    assertSpanStatusEquals("CONTENT_REMOVED attempt 3", ok());
    assertSpanException("CONTENT_REMOVED attempt (1|2)");
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("MERGE", EVENT_TYPE_KEY, MERGE.name());
    assertSpanAttributeEquals("COMMIT", EVENT_TYPE_KEY, COMMIT.name());
    assertSpanAttributeEquals("CONTENT_STORED", EVENT_TYPE_KEY, CONTENT_STORED.name());
    assertSpanAttributeEquals("CONTENT_REMOVED", EVENT_TYPE_KEY, CONTENT_REMOVED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanAttributeEquals("attempt 2", DELIVERY_ATTEMPT_KEY, 2L);
    assertSpanAttributeEquals("CONTENT_REMOVED delivery", RETRIES_KEY, 2L);
    assertSpanEventPresent("(COMMIT|CONTENT_REMOVED) delivery", "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber1, "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber2, "delivery rejected");
  }

  private void assertTransplantTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .containsExactlyInAnyOrder(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.TRANSPLANT publish",
            "nessie.events.subscribers.TRANSPLANT receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.COMMIT publish",
            "nessie.events.subscribers.COMMIT receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_STORED publish",
            "nessie.events.subscribers.CONTENT_STORED receive",
            "nessie.events.subscribers.CONTENT_STORED receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.CONTENT_REMOVED publish",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            "nessie.events.subscribers.CONTENT_REMOVED receive",
            // delivery to subscriber 1
            // delivery to subscriber 1
            "nessie.events.subscribers.TRANSPLANT delivery",
            "nessie.events.subscribers.TRANSPLANT attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.COMMIT delivery",
            "nessie.events.subscribers.COMMIT attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_STORED delivery",
            "nessie.events.subscribers.CONTENT_STORED attempt 1",
            // delivery to subscriber 1
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1",
            // delivery to subscriber 2 (rejected)
            "nessie.events.subscribers.CONTENT_STORED delivery",
            // delivery to subscriber 2 (2 failures, then success)
            "nessie.events.subscribers.CONTENT_REMOVED delivery",
            "nessie.events.subscribers.CONTENT_REMOVED attempt 1", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 2", // error
            "nessie.events.subscribers.CONTENT_REMOVED attempt 3");
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.TRANSPLANT);
    assertSpanStatusEquals("nessie.results", unset());
    assertSpanStatusEquals("delivery", ok());
    assertSpanStatusEquals("(TRANSPLANT|COMMIT|CONTENT_STORED) attempt", ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber1, ok());
    assertSpanStatusEquals("CONTENT_REMOVED attempt (1|2)", subscriber2, error());
    assertSpanStatusEquals("CONTENT_REMOVED attempt 3", ok());
    assertSpanException("CONTENT_REMOVED attempt (1|2)");
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("MERGE", EVENT_TYPE_KEY, TRANSPLANT.name());
    assertSpanAttributeEquals("COMMIT", EVENT_TYPE_KEY, COMMIT.name());
    assertSpanAttributeEquals("CONTENT_STORED", EVENT_TYPE_KEY, CONTENT_STORED.name());
    assertSpanAttributeEquals("CONTENT_REMOVED", EVENT_TYPE_KEY, CONTENT_REMOVED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanAttributeEquals("attempt 2", DELIVERY_ATTEMPT_KEY, 2L);
    assertSpanAttributeEquals("CONTENT_REMOVED delivery", RETRIES_KEY, 2L);
    assertSpanEventPresent("(COMMIT|CONTENT_REMOVED) delivery", "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber1, "delivery complete");
    assertSpanEventPresent("CONTENT_STORED delivery", subscriber2, "delivery rejected");
  }

  private void assertReferenceCreatedTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .containsExactlyInAnyOrder(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.REFERENCE_CREATED publish",
            "nessie.events.subscribers.REFERENCE_CREATED receive",
            "nessie.events.subscribers.REFERENCE_CREATED receive",
            // delivery to subscriber 1
            "nessie.events.subscribers.REFERENCE_CREATED delivery",
            "nessie.events.subscribers.REFERENCE_CREATED attempt 1",
            // delivery to subscriber 2
            "nessie.events.subscribers.REFERENCE_CREATED delivery",
            "nessie.events.subscribers.REFERENCE_CREATED attempt 1");
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.REFERENCE_CREATED);
    assertSpanStatusEquals("nessie.results", unset());
    assertSpanStatusEquals("delivery", ok());
    assertSpanStatusEquals("attempt", ok());
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("REFERENCE_CREATED", EVENT_TYPE_KEY, REFERENCE_CREATED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanEventPresent("delivery", "delivery complete");
  }

  private void assertReferenceDeletedTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .containsExactlyInAnyOrder(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.REFERENCE_DELETED publish",
            "nessie.events.subscribers.REFERENCE_DELETED receive",
            "nessie.events.subscribers.REFERENCE_DELETED receive",
            // delivery to subscriber 1
            "nessie.events.subscribers.REFERENCE_DELETED delivery",
            "nessie.events.subscribers.REFERENCE_DELETED attempt 1",
            // delivery to subscriber 2 (failed)
            "nessie.events.subscribers.REFERENCE_DELETED delivery", // error
            "nessie.events.subscribers.REFERENCE_DELETED attempt 1", // error
            "nessie.events.subscribers.REFERENCE_DELETED attempt 2", // error
            "nessie.events.subscribers.REFERENCE_DELETED attempt 3" // error
            );
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.REFERENCE_DELETED);
    assertSpanStatusEquals("nessie.results", unset());
    assertThatSpanStatus("delivery").containsOnly(ok(), error());
    assertThatSpanStatus("attempt").containsOnly(ok(), error());
    assertSpanException("delivery");
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("REFERENCE_DELETED", EVENT_TYPE_KEY, REFERENCE_DELETED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanAttributeEquals("attempt 2", DELIVERY_ATTEMPT_KEY, 2L);
    assertSpanAttributeEquals("attempt 3", DELIVERY_ATTEMPT_KEY, 3L);
    assertSpanEventPresent("delivery", subscriber1, "delivery complete");
    assertSpanEventPresent("delivery", subscriber2, "delivery failed");
  }

  private void assertReferenceUpdatedTraces(boolean authEnabled) {
    assertThat(tracer.isResolvable()).isTrue();
    assertThat(exporter.isResolvable()).isTrue();
    assertThatSpanName("nessie")
        .contains(
            // result collector (parent span)
            "nessie.results",
            // collector -> service (Vert.x)
            "nessie.events.service publish",
            "nessie.events.service receive",
            // service -> subscriber handlers (Vert.x)
            "nessie.events.subscribers.REFERENCE_UPDATED publish",
            "nessie.events.subscribers.REFERENCE_UPDATED receive",
            "nessie.events.subscribers.REFERENCE_UPDATED receive",
            // delivery to subscriber 1
            "nessie.events.subscribers.REFERENCE_UPDATED delivery",
            "nessie.events.subscribers.REFERENCE_UPDATED attempt 1",
            // delivery to subscriber 2
            "nessie.events.subscribers.REFERENCE_UPDATED delivery",
            "nessie.events.subscribers.REFERENCE_UPDATED attempt 1");
    assertEndUserId(authEnabled);
    assertPeerService();
    assertResultTypeAttribute(ResultType.REFERENCE_ASSIGNED);
    assertSpanStatusEquals("nessie.results", unset());
    assertSpanStatusEquals("delivery", ok());
    assertSpanStatusEquals("attempt", ok());
    assertSpanAttributePresent("nessie.events", EVENT_ID_KEY);
    assertSpanAttributePresent("nessie.events", SUBSCRIPTION_ID_KEY);
    assertSpanAttributeEquals("REFERENCE_CREATED", EVENT_TYPE_KEY, REFERENCE_CREATED.name());
    assertSpanAttributeEquals("attempt 1", DELIVERY_ATTEMPT_KEY, 1L);
    assertSpanEventPresent("delivery", "delivery complete");
  }

  private void assertEndUserId(boolean authEnabled) {
    assertSpanAttributeEquals("nessie", ENDUSER_ID, authEnabled ? "Alice" : null);
  }

  private void assertPeerService() {
    assertSpanAttributeEquals("nessie", SERVICE_NAME, "Nessie");
  }

  private void assertResultTypeAttribute(ResultType resultType) {
    assertSpanAttributeEquals(
        "nessie.results", NESSIE_RESULT_TYPE_ATTRIBUTE_KEY, resultType.name());
  }

  private void assertSpanAttributePresent(
      @SuppressWarnings("SameParameterValue") String nameRegex, AttributeKey<?> attributeKey) {
    assertThatSpanAttribute(nameRegex, attributeKey).allSatisfy(a -> assertThat(a).isNotNull());
  }

  private <T> void assertSpanAttributeEquals(
      String nameRegex, AttributeKey<T> attributeKey, T expected) {
    assertThatSpanAttribute(nameRegex, attributeKey)
        .allSatisfy(a -> assertThat(a).isEqualTo(expected));
  }

  private void assertSpanException(String nameRegex) {
    assertThatSpanExceptionAttribute(nameRegex)
        .allSatisfy(e -> assertThat(e).contains(RuntimeException.class.getSimpleName()));
  }

  private void assertSpanStatusEquals(String nameRegex, StatusData expected) {
    assertThatSpanStatus(nameRegex).allSatisfy(s -> assertThat(s).isEqualTo(expected));
  }

  private void assertSpanStatusEquals(
      @SuppressWarnings("SameParameterValue") String nameRegex,
      MockEventSubscriber subscriber,
      StatusData status) {
    assertThatSpan(nameRegex)
        .filteredOn(subscriptionKeyFilter(subscriber))
        .extracting(SpanData::getStatus)
        .allSatisfy(s -> assertThat(s).isEqualTo(status));
  }

  private void assertSpanEventPresent(
      String nameRegex, MockEventSubscriber subscriber, String event) {
    assertThatSpan(nameRegex, subscriber)
        .flatExtracting(SpanData::getEvents)
        .extracting(EventData::getName)
        .anySatisfy(e -> assertThat(e).isEqualTo(event));
  }

  private void assertSpanEventPresent(
      String nameRegex, @SuppressWarnings("SameParameterValue") String event) {
    assertThatSpanEventName(nameRegex).anySatisfy(e -> assertThat(e).isEqualTo(event));
  }

  private AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>>
      assertThatSpanName(@SuppressWarnings("SameParameterValue") String nameRegex) {
    return assertThatSpan(nameRegex).extracting(SpanData::getName);
  }

  private AbstractListAssert<?, List<? extends StatusData>, StatusData, ObjectAssert<StatusData>>
      assertThatSpanStatus(String nameRegex) {
    return assertThatSpan(nameRegex).extracting(SpanData::getStatus);
  }

  private <T> AbstractListAssert<?, List<? extends T>, T, ObjectAssert<T>> assertThatSpanAttribute(
      String nameRegex, AttributeKey<T> attributeKey) {
    return assertThatSpan(nameRegex)
        .extracting(SpanData::getAttributes)
        .filteredOn(a -> a.get(attributeKey) != null)
        .extracting(a -> a.get(attributeKey));
  }

  private AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>>
      assertThatSpanExceptionAttribute(String nameRegex) {
    return assertThatSpanEvent(nameRegex)
        .filteredOn(e -> e.getName().contains("exception"))
        .extracting(e -> e.getAttributes().get(EXCEPTION_TYPE));
  }

  private AbstractListAssert<?, List<? extends String>, String, ObjectAssert<String>>
      assertThatSpanEventName(@SuppressWarnings("SameParameterValue") String nameRegex) {
    return assertThatSpanEvent(nameRegex).extracting(EventData::getName);
  }

  private AbstractListAssert<?, List<? extends EventData>, EventData, ObjectAssert<EventData>>
      assertThatSpanEvent(String nameRegex) {
    return assertThatSpan(nameRegex).flatExtracting(SpanData::getEvents);
  }

  private ListAssert<SpanData> assertThatSpan(String nameRegex, MockEventSubscriber subscriber) {
    return assertThatSpan(nameRegex).filteredOn(subscriptionKeyFilter(subscriber));
  }

  private ListAssert<SpanData> assertThatSpan(String nameRegex) {
    return assertThat(exporter.get().getSpans())
        .filteredOn(s -> Pattern.compile(nameRegex).matcher(s.getName()).find());
  }

  private static Predicate<SpanData> subscriptionKeyFilter(MockEventSubscriber subscriber) {
    return span ->
        Objects.equals(
            span.getAttributes().get(SUBSCRIPTION_ID_KEY),
            subscriber.getSubscription().getIdAsText());
  }
}
