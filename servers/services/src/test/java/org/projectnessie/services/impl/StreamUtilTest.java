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
package org.projectnessie.services.impl;

import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class StreamUtilTest {

  @Test
  public void testTakeUntil() {
    Assertions.assertThat(
            StreamSupport.stream(
                StreamUtil.takeUntilIncl(
                    IntStream.rangeClosed(1, 10).spliterator(), x -> x.equals(7)),
                false))
        .hasSize(7)
        .containsExactly(1, 2, 3, 4, 5, 6, 7);

    Assertions.assertThat(
            StreamSupport.stream(
                StreamUtil.takeUntilIncl(
                    IntStream.rangeClosed(1, 10).spliterator(), x -> x.equals(3)),
                false))
        .hasSize(3)
        .containsExactly(1, 2, 3);

    Assertions.assertThat(
            StreamSupport.stream(
                StreamUtil.takeUntilIncl(
                    IntStream.rangeClosed(1, 1).spliterator(), x -> x.equals(2)),
                false))
        .hasSize(1)
        .containsExactly(1);
  }

  @Test
  public void testTakeUntilWithNonMatchingPredicate() {
    Assertions.assertThat(
            StreamSupport.stream(
                StreamUtil.takeUntilIncl(
                    IntStream.rangeClosed(1, 5).spliterator(), x -> x.equals(6)),
                false))
        .hasSize(5)
        .containsExactly(1, 2, 3, 4, 5);

    Assertions.assertThat(
            StreamSupport.stream(
                StreamUtil.takeUntilIncl(
                    IntStream.rangeClosed(1, 5).spliterator(), x -> x.equals(0)),
                false))
        .hasSize(5)
        .containsExactly(1, 2, 3, 4, 5);
  }
}
