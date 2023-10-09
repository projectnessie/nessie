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
package org.projectnessie.versioned.storage.common.persist;

import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Collections.emptyList;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.threeten.extra.MutableClock;

@ExtendWith(SoftAssertionsExtension.class)
public class TestReference {

  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void forNewPointerNoChange() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("GMT"));
    StoreConfig config =
        StoreConfig.Adjustable.empty()
            .withClock(clock)
            .withReferencePreviousHeadCount(1000)
            .withReferencePreviousHeadTimeSpanSeconds(300);

    Reference original =
        reference("foo", randomObjId(), false, config.currentTimeMicros(), null, emptyList());

    clock.add(1, SECONDS);
    Reference current = original.forNewPointer(original.pointer(), config);
    soft.assertThat(current.previousPointers()).isEmpty();

    List<Reference.PreviousPointer> expected = new ArrayList<>();
    clock.add(1, SECONDS);
    expected.add(
        Reference.PreviousPointer.previousPointer(current.pointer(), config.currentTimeMicros()));
    current = current.forNewPointer(randomObjId(), config);
    soft.assertThat(current.previousPointers()).containsExactlyElementsOf(expected);

    clock.add(1, SECONDS);
    current = current.forNewPointer(current.pointer(), config);
    soft.assertThat(current.previousPointers()).containsExactlyElementsOf(expected);
  }

  @Test
  public void forNewPointerTimeLimit() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("GMT"));
    int secondsTimeSpan = 3;
    StoreConfig config =
        StoreConfig.Adjustable.empty()
            .withClock(clock)
            .withReferencePreviousHeadCount(1000)
            .withReferencePreviousHeadTimeSpanSeconds(secondsTimeSpan);

    Reference current =
        reference("foo", randomObjId(), false, config.currentTimeMicros(), null, emptyList());
    for (int i = 0; i < secondsTimeSpan; i++) {
      clock.add(1, SECONDS);
      current = current.forNewPointer(randomObjId(), config);
    }

    List<Reference.PreviousPointer> expected = new ArrayList<>();
    for (int i = 0; i < secondsTimeSpan + 1; i++) {
      clock.add(1, SECONDS);
      expected.add(
          Reference.PreviousPointer.previousPointer(current.pointer(), config.currentTimeMicros()));
      current = current.forNewPointer(randomObjId(), config);
    }

    Collections.reverse(expected);
    soft.assertThat(current.previousPointers()).containsExactlyElementsOf(expected);
  }

  @Test
  public void forNewPointerSizeLimit() {
    MutableClock clock = MutableClock.of(Instant.now(), ZoneId.of("GMT"));
    int sizeLimit = 3;
    StoreConfig config =
        StoreConfig.Adjustable.empty()
            .withClock(clock)
            .withReferencePreviousHeadCount(sizeLimit)
            .withReferencePreviousHeadTimeSpanSeconds(300);

    Reference current =
        reference("foo", randomObjId(), false, config.currentTimeMicros(), null, emptyList());
    for (int i = 0; i < sizeLimit; i++) {
      clock.add(1, SECONDS);
      current = current.forNewPointer(randomObjId(), config);
    }

    List<Reference.PreviousPointer> expected = new ArrayList<>();
    for (int i = 0; i < sizeLimit; i++) {
      clock.add(1, SECONDS);
      expected.add(
          Reference.PreviousPointer.previousPointer(current.pointer(), config.currentTimeMicros()));
      current = current.forNewPointer(randomObjId(), config);
    }

    Collections.reverse(expected);
    soft.assertThat(current.previousPointers()).containsExactlyElementsOf(expected);
  }
}
