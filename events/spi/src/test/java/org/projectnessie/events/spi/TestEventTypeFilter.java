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
package org.projectnessie.events.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.EnumSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.projectnessie.events.api.EventType;

class TestEventTypeFilter {

  @ParameterizedTest
  @EnumSource(EventType.class)
  void all(EventType t) {
    assertThat(EventTypeFilter.all().test(t)).isTrue();
  }

  @ParameterizedTest
  @EnumSource(EventType.class)
  void none(EventType t) {
    assertThat(EventTypeFilter.none().test(t)).isFalse();
  }

  @Test
  void ofSingle() {
    EventTypeFilter filter = EventTypeFilter.of(EventType.COMMIT);
    assertThat(filter.test(EventType.COMMIT)).isTrue();
    EnumSet.complementOf(EnumSet.of(EventType.COMMIT))
        .forEach(t -> assertThat(filter.test(t)).isFalse());
  }

  @Test
  void ofVarargs() {
    EventTypeFilter filter = EventTypeFilter.of(EventType.COMMIT, EventType.MERGE);
    assertThat(filter.test(EventType.COMMIT)).isTrue();
    assertThat(filter.test(EventType.MERGE)).isTrue();
    EnumSet.complementOf(EnumSet.of(EventType.COMMIT, EventType.MERGE))
        .forEach(t -> assertThat(filter.test(t)).isFalse());
  }

  @Test
  void ofCollection() {
    EventTypeFilter filter = EventTypeFilter.of(EnumSet.of(EventType.COMMIT, EventType.MERGE));
    assertThat(filter.test(EventType.COMMIT)).isTrue();
    assertThat(filter.test(EventType.MERGE)).isTrue();
    EnumSet.complementOf(EnumSet.of(EventType.COMMIT, EventType.MERGE))
        .forEach(t -> assertThat(filter.test(t)).isFalse());
  }

  @Test
  void ofCollectionEmpty() {
    EventTypeFilter filter = EventTypeFilter.of(EnumSet.noneOf(EventType.class));
    EnumSet.allOf(EventType.class).forEach(t -> assertThat(filter.test(t)).isFalse());
  }

  @Test
  void ofCollectionSingle() {
    EventTypeFilter filter = EventTypeFilter.of(EnumSet.of(EventType.COMMIT));
    assertThat(filter.test(EventType.COMMIT)).isTrue();
    EnumSet.complementOf(EnumSet.of(EventType.COMMIT))
        .forEach(t -> assertThat(filter.test(t)).isFalse());
  }
}
