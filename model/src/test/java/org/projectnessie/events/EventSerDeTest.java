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
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Operation;

public class EventSerDeTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final CommitEvent BOB_COMMIT =
      CommitEvent.builder()
          .reference(mainBranch())
          .metadata(bobCommitMeta())
          .newHash(randomHash())
          .addOperations(bobbyTablesDelete())
          .build();

  @Test
  public void canSerDeCommitEvent() throws Exception {
    String serialized = MAPPER.writeValueAsString(BOB_COMMIT);

    assertEquals(BOB_COMMIT, MAPPER.readValue(serialized, CommitEvent.class));
  }

  @Test
  public void commitEventsContainTypeInformation() throws Exception {
    String serialized = MAPPER.writeValueAsString(BOB_COMMIT);

    JsonNode jsonNode = MAPPER.readValue(serialized, JsonNode.class);

    assertEquals("COMMIT_EVENT", jsonNode.get("type").asText());
  }

  private static Branch mainBranch() {
    return Branch.of("main", randomHash());
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
