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
package org.projectnessie.events.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TestReferenceEvent {

  @Test
  void branch() {
    ReferenceEvent event =
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .sentBy("Nessie")
            .referenceName("branch1")
            .fullReferenceName("fullRef1")
            .referenceType(ReferenceEvent.BRANCH)
            .hashAfter("hash1")
            .build();
    assertThat(event.isBranch()).isTrue();
    assertThat(event.isTag()).isFalse();
  }

  @Test
  void tag() {
    ReferenceEvent event =
        ImmutableReferenceCreatedEvent.builder()
            .id(UUID.randomUUID())
            .repositoryId("repo1")
            .createdAt(Instant.now())
            .sentBy("Nessie")
            .referenceName("tag1")
            .fullReferenceName("fullRef1")
            .referenceType(ReferenceEvent.TAG)
            .hashAfter("hash1")
            .build();
    assertThat(event.isBranch()).isFalse();
    assertThat(event.isTag()).isTrue();
  }
}
