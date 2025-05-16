/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.quarkus.tests.profiles.BaseConfigProfile.TEST_REPO_ID;

import com.google.common.collect.ImmutableMap;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.ext.NessieApiVersion;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content.Type;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.MergeResponse;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;
import org.projectnessie.model.UDF;
import org.projectnessie.server.events.fixtures.MockEventSubscriber;

@ExtendWith(QuarkusNessieClientResolver.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractQuarkusEvents {

  private final Put putInitial = Put.of(ContentKey.of("foo"), UDF.udf("udf-meta", "42", "666"));
  private final Put put = Put.of(ContentKey.of("key1"), IcebergTable.of("somewhere", 1, 1, 3, 4));
  private final CommitMeta commitMeta = CommitMeta.fromMessage("test commit");

  private URI clientUri;
  private NessieApiV1 api;

  @SuppressWarnings("CdiInjectionPointsInspection")
  @Inject
  public MockEventSubscriber subscriber;

  @BeforeAll
  void startRecording() {
    subscriber.startRecording();
  }

  @AfterAll
  void stopRecording() {
    subscriber.stopRecording();
    subscriber.reset();
  }

  @BeforeEach
  void reset() {
    subscriber.reset();
    clearMetrics();
  }

  @BeforeEach
  void initApi(NessieClientFactory clientFactory) {
    this.api = clientFactory.make(this::customizeClient);
  }

  @BeforeEach
  void setRestUri(@NessieClientUri URI uri) {
    clientUri = uri;
    RestAssured.baseURI = uri.toString();
    RestAssured.port = uri.getPort();
  }

  protected NessieClientBuilder customizeClient(
      NessieClientBuilder builder, NessieApiVersion apiVersion) {
    return builder;
  }

  protected String eventInitiator() {
    return null;
  }

  protected abstract boolean eventsEnabled();

  protected abstract void clearMetrics();

  @Test
  void smokeTestCommit() throws Exception {
    Branch branch = createBranch("branch" + System.nanoTime());
    String hashBefore = branch.getHash();
    branch =
        api.commitMultipleOperations()
            .branch(branch)
            .operation(put)
            .commitMeta(commitMeta)
            .commit();
    String hashAfter = branch.getHash();
    checkEventsForCommitResult(branch, hashBefore, hashAfter);
  }

  @Test
  void smokeTestMerge() throws Exception {
    SimpleEntry<Branch, Branch> result = prepareMergeTransplant();
    Branch source = result.getKey();
    Branch target = result.getValue();
    String hashBefore = target.getHash();
    MergeResponse response = api.mergeRefIntoBranch().fromRef(source).branch(target).merge();
    String hashAfter = response.getResultantTargetHash();
    checkEventsForMergeResult(source, target, hashBefore, hashAfter);
  }

  @Test
  void smokeTestTransplant() throws Exception {
    SimpleEntry<Branch, Branch> result = prepareMergeTransplant();
    Branch source = result.getKey();
    Branch target = result.getValue();
    String hashBefore = target.getHash();
    MergeResponse response =
        api.transplantCommitsIntoBranch()
            .fromRefName(source.getName())
            .hashesToTransplant(Collections.singletonList(source.getHash()))
            .branch(target)
            .transplant();
    String hashAfter = response.getResultantTargetHash();
    checkEventsForTransplantResult(target, hashBefore, hashAfter);
  }

  @Test
  void smokeTestReferenceCreated() throws Exception {
    Reference reference = Branch.of("branch" + System.nanoTime(), mainBranch().getHash());
    Branch branch = (Branch) api.createReference().reference(reference).create();
    checkEventsForReferenceCreatedResult(branch);
  }

  @Test
  void smokeTestReferenceDeleted() throws Exception {
    Branch branch = createBranch("branch" + System.nanoTime());
    api.deleteBranch().branch(branch).delete();
    checkEventsForReferenceDeletedResult(branch);
  }

  @Test
  void smokeTestReferenceUpdated() throws Exception {
    Map.Entry<Branch, Branch> result = prepareReferenceUpdated();
    api.assignBranch().branch(result.getKey()).assignTo(result.getValue()).assign();
    checkEventsForReferenceAssignedResult(result.getKey(), result.getValue());
  }

  private void checkEventsForCommitResult(Branch branch, String hashBefore, String hashAfter) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(2);
      checkCommitEvent(events, branch, hashBefore, hashAfter);
      checkContentStoredEvent(events);
      checkCommitMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkEventsForMergeResult(
      Branch source, Branch target, String hashBefore, String hashAfter) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(3);
      checkMergeEvent(events, source, target, hashBefore, hashAfter);
      checkCommitEvent(events, target, hashBefore, hashAfter);
      checkContentStoredEvent(events);
      checkMergeMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkEventsForTransplantResult(Branch target, String hashBefore, String hashAfter) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(3);
      checkTransplantEvent(events, target, hashBefore, hashAfter);
      checkCommitEvent(events, target, hashBefore, hashAfter);
      checkContentStoredEvent(events);
      checkTransplantMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkEventsForReferenceCreatedResult(Branch branch) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(1);
      checkReferenceCreatedEvent(events, branch);
      checkReferenceCreatedMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkEventsForReferenceDeletedResult(Branch branch) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(1);
      checkReferenceDeletedEvent(events, branch);
      checkReferenceDeletedMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkEventsForReferenceAssignedResult(Branch branch1, Branch branch2) {
    if (eventsEnabled()) {
      List<Event> events = subscriber.awaitEvents(1);
      checkReferenceUpdatedEvent(events, branch1, branch2);
      checkReferenceUpdatedMetrics();
    } else {
      checkNoEvents();
      checkNoMetrics();
    }
  }

  private void checkCommitEvent(
      List<Event> events, Branch branch, String hashBefore, String hashAfter) {
    CommitEvent commitEvent = findEvent(events, CommitEvent.class);
    assertThat(commitEvent)
        .isNotNull()
        .extracting(
            CommitEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            CommitEvent::getProperties,
            CommitEvent::getHashBefore,
            CommitEvent::getHashAfter,
            e -> e.getReference().getName())
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            hashBefore,
            hashAfter,
            branch.getName());
  }

  private void checkMergeEvent(
      List<Event> events, Branch source, Branch target, String hashBefore, String hashAfter) {
    MergeEvent mergeEvent = findEvent(events, MergeEvent.class);
    assertThat(mergeEvent)
        .isNotNull()
        .extracting(
            MergeEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            MergeEvent::getProperties,
            MergeEvent::getHashBefore,
            MergeEvent::getHashAfter,
            e -> e.getSourceReference().getName(),
            e -> e.getTargetReference().getName())
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            hashBefore,
            hashAfter,
            source.getName(),
            target.getName());
  }

  private void checkTransplantEvent(
      List<Event> events, Branch target, String hashBefore, String hashAfter) {
    TransplantEvent transplantEvent = findEvent(events, TransplantEvent.class);
    assertThat(transplantEvent)
        .isNotNull()
        .extracting(
            TransplantEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            TransplantEvent::getProperties,
            TransplantEvent::getHashBefore,
            TransplantEvent::getHashAfter,
            e -> e.getTargetReference().getName())
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            hashBefore,
            hashAfter,
            target.getName());
  }

  private void checkContentStoredEvent(List<Event> events) {
    ContentStoredEvent contentStoredEvent = findEvent(events, ContentStoredEvent.class);
    assertThat(contentStoredEvent)
        .isNotNull()
        .extracting(
            ContentStoredEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            ContentStoredEvent::getProperties,
            e -> e.getContentKey().getName(),
            e -> e.getContent().getType())
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            "key1",
            Type.ICEBERG_TABLE);
  }

  private void checkReferenceCreatedEvent(List<Event> events, Branch branch) {
    ReferenceCreatedEvent event = findEvent(events, ReferenceCreatedEvent.class);
    assertThat(event)
        .isNotNull()
        .extracting(
            ReferenceCreatedEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            ReferenceCreatedEvent::getProperties,
            e -> e.getReference().getName(),
            ReferenceCreatedEvent::getHashAfter)
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            branch.getName(),
            branch.getHash());
  }

  private void checkReferenceDeletedEvent(List<Event> events, Branch branch) {
    ReferenceDeletedEvent event = findEvent(events, ReferenceDeletedEvent.class);
    assertThat(event)
        .isNotNull()
        .extracting(
            ReferenceDeletedEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            ReferenceDeletedEvent::getProperties,
            e -> e.getReference().getName(),
            ReferenceDeletedEvent::getHashBefore)
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            branch.getName(),
            branch.getHash());
  }

  private void checkReferenceUpdatedEvent(List<Event> events, Branch source, Branch target) {
    ReferenceUpdatedEvent event = findEvent(events, ReferenceUpdatedEvent.class);
    assertThat(event)
        .isNotNull()
        .extracting(
            ReferenceUpdatedEvent::getRepositoryId,
            e -> e.getEventInitiator().orElse(null),
            ReferenceUpdatedEvent::getProperties,
            ReferenceUpdatedEvent::getHashBefore,
            ReferenceUpdatedEvent::getHashAfter)
        .containsExactly(
            TEST_REPO_ID,
            eventInitiator(),
            ImmutableMap.of("foo", "bar"),
            source.getHash(),
            target.getHash());
  }

  private void checkNoEvents() {
    assertThat(subscriber.getEvents()).isEmpty();
  }

  private void checkNoMetrics() {
    String metrics = getMetrics();
    assertThat(metrics).doesNotContain("nessie_events_successful", "nessie_events_total");
  }

  private void checkCommitMetrics() {
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"COMMIT\"} 1.0",
            "nessie_events_successful_total{application=\"Nessie\",type=\"CONTENT_STORED\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"COMMIT\"",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"CONTENT_STORED\"");
  }

  private void checkMergeMetrics() {
    checkCommitMetrics();
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"MERGE\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"MERGE\"");
  }

  private void checkTransplantMetrics() {
    checkCommitMetrics();
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"TRANSPLANT\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"TRANSPLANT\"");
  }

  private void checkReferenceCreatedMetrics() {
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"REFERENCE_CREATED\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"REFERENCE_CREATED\"");
  }

  private void checkReferenceDeletedMetrics() {
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"REFERENCE_DELETED\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"REFERENCE_DELETED\"");
  }

  private void checkReferenceUpdatedMetrics() {
    String metrics = getMetrics();
    assertThat(metrics)
        .contains(
            "nessie_events_successful_total{application=\"Nessie\",type=\"REFERENCE_UPDATED\"} 1.0",
            "nessie_events_total_seconds_bucket{application=\"Nessie\",status=\"SUCCESSFUL\",type=\"REFERENCE_UPDATED\"");
  }

  private SimpleEntry<Branch, Branch> prepareMergeTransplant()
      throws NessieNotFoundException, NessieConflictException {
    Branch source = createBranch("source" + System.nanoTime());
    // common ancestor
    source =
        api.commitMultipleOperations()
            .branch(source)
            .commitMeta(commitMeta)
            .operation(putInitial)
            .commit();
    if (eventsEnabled()) {
      subscriber.awaitEvents(2);
      reset();
    }
    Branch target = createBranch("target" + System.nanoTime(), source);
    source =
        api.commitMultipleOperations()
            .branch(source)
            .operation(put)
            .commitMeta(commitMeta)
            .commit();
    if (eventsEnabled()) {
      subscriber.awaitEvents(2); // COMMIT + CONTENT_STORED
      reset();
    }
    return new SimpleEntry<>(source, target);
  }

  private SimpleEntry<Branch, Branch> prepareReferenceUpdated()
      throws NessieNotFoundException, NessieConflictException {
    Branch branch1 = createBranch("branch1-" + System.nanoTime());
    Branch branch2 = createBranch("branch2-" + System.nanoTime());
    return new SimpleEntry<>(branch1, branch2);
  }

  private <E extends Event> E findEvent(List<Event> events, Class<? extends E> eventClass) {
    for (Event event : events) {
      if (eventClass.isInstance(event)) {
        return eventClass.cast(event);
      }
    }
    return null;
  }

  private Branch createBranch(String name) throws NessieNotFoundException, NessieConflictException {
    return createBranch(name, mainBranch());
  }

  private Branch createBranch(String name, Branch source)
      throws NessieNotFoundException, NessieConflictException {
    Reference reference = Branch.of(name, source.getHash());
    Branch branch = (Branch) api.createReference().reference(reference).create();
    if (eventsEnabled()) {
      subscriber.awaitEvents(1);
      reset();
    }
    return branch;
  }

  private Branch mainBranch() throws NessieNotFoundException {
    return (Branch) api.getReference().refName("main").get();
  }

  private String getMetrics() {
    int managementPort = Integer.getInteger("quarkus.management.port");
    URI managementBaseUri;
    try {
      managementBaseUri =
          new URI("http", null, clientUri.getHost(), managementPort, "/", null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return RestAssured.given()
        .when()
        .baseUri(managementBaseUri.toString())
        .basePath("/q/metrics")
        .accept("*/*")
        .get()
        .then()
        .statusCode(200)
        .extract()
        .asString();
  }
}
