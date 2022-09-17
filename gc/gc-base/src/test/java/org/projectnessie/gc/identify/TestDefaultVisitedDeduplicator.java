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
package org.projectnessie.gc.identify;

import java.time.Instant;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestDefaultVisitedDeduplicator {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void foo() {
    DefaultVisitedDeduplicator dedup = new DefaultVisitedDeduplicator();

    Instant t = Instant.now();
    Instant minus1 = t.minusSeconds(1);
    Instant minus2 = t.minusSeconds(2);

    soft.assertThat(dedup.alreadyVisited(t, "commit-1")).isFalse();
    // commit-1 already visited with same cut-off timestamp --> true
    soft.assertThat(dedup.alreadyVisited(t, "commit-1")).isTrue();

    // commit-1 has been visited with cut-off timestamp T, which is newer than tMinus2,
    // commit log scanning must continue --> false
    soft.assertThat(dedup.alreadyVisited(minus2, "commit-1")).isFalse();
    // commit-1 already visited with cut-off timestamp tMinus2 --> true
    soft.assertThat(dedup.alreadyVisited(minus2, "commit-1")).isTrue();

    // commit-1 already visited with cut-off timestamp tMinus2, which is _older_ than
    // tMinus1 --> can stop commit-log scanning --> true
    soft.assertThat(dedup.alreadyVisited(minus1, "commit-1")).isTrue();

    // commit-2 has never been visited
    soft.assertThat(dedup.alreadyVisited(t, "commit-2")).isFalse();

    // commit-3 has never been visited
    soft.assertThat(dedup.alreadyVisited(minus2, "commit-3")).isFalse();
    // commit-3 has been visited at T-2, which includes T-1 --> true
    soft.assertThat(dedup.alreadyVisited(minus1, "commit-3")).isTrue();
  }
}
