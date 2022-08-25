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
package org.projectnessie.versioned.transfer;

import static java.util.Collections.reverse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestBoundedList {
  @Test
  public void empty() {
    BoundedList<String> l = new BoundedList<>(20);
    verifyEmpty(l);
  }

  private static void verifyEmpty(BoundedList<String> l) {
    assertThat(l.size()).isEqualTo(0);
    assertThatThrownBy(l::leastRecentElement).isInstanceOf(IllegalStateException.class);

    List<String> consumed = new ArrayList<>();
    l.newestToOldest(consumed::add);
    assertThat(consumed).isEmpty();

    consumed.clear();
    l.oldestToNewest(consumed::add);
    assertThat(consumed).isEmpty();

    assertThatThrownBy(l::removeLeastRecent).isInstanceOf(IllegalStateException.class);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 10, 20, 50})
  public void lifecycleLong(int capacity) {
    BoundedList<String> l = new BoundedList<>(capacity);
    List<String> reference = new ArrayList<>(capacity);

    for (int i = 0; i < 100; ) {
      String value = "value-" + i;
      l.add(value);
      if (reference.size() == capacity) {
        reference.remove(0);
      }
      reference.add(value);
      i++;

      int size = Math.min(capacity, i);
      assertThat(l.size()).isEqualTo(size);
      assertThat(l.leastRecentElement()).isEqualTo(reference.get(0));

      List<String> consumed = new ArrayList<>();
      l.oldestToNewest(consumed::add);
      assertThat(consumed).hasSize(size).containsExactlyElementsOf(reference);

      consumed.clear();
      l.newestToOldest(consumed::add);
      reverse(consumed);
      assertThat(consumed).hasSize(size).containsExactlyElementsOf(reference);

      // Test "drain" functionality
      BoundedList<String> copy = l.copy();
      for (int j = 1; j < size; j++) {
        String expectRemoved = reference.get(j - 1);
        String oldest = reference.get(j);
        assertThat(copy.removeLeastRecent()).isEqualTo(expectRemoved);
        assertThat(copy.size()).isEqualTo(size - j);
        assertThat(copy.leastRecentElement()).isEqualTo(oldest);

        consumed.clear();
        copy.oldestToNewest(consumed::add);
        assertThat(consumed)
            .hasSize(size - j)
            .containsExactlyElementsOf(reference.subList(j, size));

        consumed.clear();
        copy.newestToOldest(consumed::add);
        reverse(consumed);
        assertThat(consumed)
            .hasSize(size - j)
            .containsExactlyElementsOf(reference.subList(j, size));
      }
      String leastRecent = reference.get(reference.size() - 1);
      assertThat(copy.leastRecentElement())
          .as("for i == %d, size = %d", i, size)
          .isEqualTo(leastRecent);
      assertThat(copy.removeLeastRecent())
          .as("for i == %d, size = %d", i, size)
          .isEqualTo(leastRecent);
      verifyEmpty(copy);
    }
  }
}
