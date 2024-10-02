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
package org.projectnessie.versioned.storage.cleanup;

import static org.projectnessie.versioned.storage.common.persist.ObjId.objIdFromByteArray;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestReferencedObjectsFilterImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void emptyFilterContainsNothing() {
    ReferencedObjectsFilterImpl filter =
        new ReferencedObjectsFilterImpl(CleanupParams.builder().build());
    soft.assertThat(filter.isProbablyReferenced(ObjId.EMPTY_OBJ_ID)).isFalse();
    for (int i = 0; i < 100; i++) {
      ObjId id = randomObjId();
      soft.assertThat(filter.isProbablyReferenced(id)).describedAs("id = %s", id).isFalse();
    }
  }

  @Test
  public void filterContainsAdded() {
    ReferencedObjectsFilterImpl filter =
        new ReferencedObjectsFilterImpl(CleanupParams.builder().build());

    soft.assertThat(filter.estimatedHeapPressure()).isGreaterThan(1L);

    soft.assertThat(filter.markReferenced(ObjId.EMPTY_OBJ_ID)).isTrue();
    soft.assertThat(filter.markReferenced(ObjId.EMPTY_OBJ_ID)).isFalse();

    Set<ObjId> ids = new HashSet<>(3000);
    for (int i = 0; i < 1000; i++) {
      ids.add(randomObjId());
    }

    for (int i = 0; i < 1000; i++) {
      byte[] bytes = new byte[4 + ThreadLocalRandom.current().nextInt(33)];
      ThreadLocalRandom.current().nextBytes(bytes);
      ids.add(objIdFromByteArray(bytes));
    }

    for (ObjId id : ids) {
      // There is a theoretical chance that this assertion fails, but that change is extremely low.
      // (We're adding 2000 object IDs to a bloom filter with an expected object count of 1M and a
      // low FPP.)
      soft.assertThat(filter.markReferenced(id)).isTrue();
      soft.assertThat(filter.markReferenced(id)).isFalse();
    }

    soft.assertThat(filter.isProbablyReferenced(ObjId.EMPTY_OBJ_ID)).isTrue();
    for (ObjId id : ids) {
      soft.assertThat(filter.isProbablyReferenced(id)).describedAs("id = %s", id).isTrue();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {100, 1_000, 10_000})
  public void withinExpectedFpp(int expected) {
    ReferencedObjectsFilterImpl filter =
        new ReferencedObjectsFilterImpl(CleanupParams.builder().expectedObjCount(expected).build());

    for (int i = 0; i < expected; i++) {
      ObjId id = randomObjId();
      soft.assertThatCode(() -> filter.markReferenced(id)).doesNotThrowAnyException();
      soft.assertThat(filter.withinExpectedFpp()).isTrue();
    }

    // "withinExpectedFpp" should trigger at some point
    boolean thrown = false;
    for (int i = 0; i < expected / 2; i++) {
      ObjId id = randomObjId();
      try {
        filter.markReferenced(id);
        soft.assertThat(filter.withinExpectedFpp()).isTrue();
      } catch (MustRestartWithBiggerFilterRuntimeException e) {
        soft.assertThat(filter.withinExpectedFpp()).isFalse();
        thrown = true;
        break;
      }
    }
    soft.assertThat(thrown).isTrue();
  }
}
