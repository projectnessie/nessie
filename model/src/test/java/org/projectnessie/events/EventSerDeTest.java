/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;

public class EventSerDeTest {
  private static final Instant EVENT_TIME = Instant.ofEpochMilli(1234634950000L);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final CommitEvent BOB_COMMIT =
      CommitEvent.builder()
          .reference(mainBranch())
          .metadata(bobCommitMeta())
          .newHash(randomHash())
          .addOperations(bobbyTablesDelete())
          .eventTime(EVENT_TIME)
          .build();

  private static final ReferenceCreatedEvent DEV_CREATION =
      ReferenceCreatedEvent.builder().reference(devBranch()).eventTime(EVENT_TIME).build();

  private static final ReferenceDeletedEvent MAIN_DELETION =
      ReferenceDeletedEvent.builder()
          .referenceName("main")
          .referenceType(Reference.ReferenceType.BRANCH)
          .eventTime(EVENT_TIME)
          .build();

  private static Stream<Arguments> eventsToType() {
    return Stream.of(
        Arguments.of(BOB_COMMIT, "COMMIT_EVENT"),
        Arguments.of(DEV_CREATION, "REFERENCE_CREATED"),
        Arguments.of(MAIN_DELETION, "REFERENCE_DELETED"));
  }

  private static Stream<Arguments> eventsToClass() {
    return Stream.of(
        Arguments.of(BOB_COMMIT, CommitEvent.class),
        Arguments.of(DEV_CREATION, ReferenceCreatedEvent.class),
        Arguments.of(MAIN_DELETION, ReferenceDeletedEvent.class));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("eventsToType")
  public void eventsContainRelevantInformation(Event event, String expectedType) throws Exception {
    String serialized = MAPPER.writeValueAsString(event);

    JsonNode jsonNode = MAPPER.readValue(serialized, JsonNode.class);

    assertEquals(expectedType, jsonNode.get("type").asText());
    assertEquals("2009-02-14T18:09:10Z", jsonNode.get("eventTime").asText());
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("eventsToClass")
  public void canSerDeCommitEvent(Event event, Class<Event> klass) throws Exception {
    String serialized = MAPPER.writeValueAsString(event);

    assertEquals(event, MAPPER.readValue(serialized, klass));
  }

  private static Branch mainBranch() {
    return Branch.of("main", randomHash());
  }

  private static Branch devBranch() {
    return Branch.of("dev", randomHash());
  }

  private static String randomHash() {
    StringBuilder sb = new StringBuilder();
    Random random = ThreadLocalRandom.current();
    while (sb.length() < 16) {
      sb.append(Integer.toHexString(random.nextInt(16)));
    }
    return sb.toString();
  }

  private static CommitMeta bobCommitMeta() {
    return CommitMeta.builder()
        .author("Bob")
        .commitTime(Instant.ofEpochMilli(356136300000L))
        .message("little bobby tables")
        .build();
  }

  private static Operation bobbyTablesDelete() {
    return Operation.Delete.of(ContentKey.of("tables", "bobby"));
  }
}
