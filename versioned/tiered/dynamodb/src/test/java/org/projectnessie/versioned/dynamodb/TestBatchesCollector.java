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
package org.projectnessie.versioned.dynamodb;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.dynamodb.DynamoStore.BatchesCollector;

public class TestBatchesCollector {

  static Integer[][] testCombinations() {
    return new Integer[][] {
      {5, 0, 0},
      {3, 0, 0},
      {5, 1, 1},
      {5, 4, 1},
      {5, 5, 1},
      {5, 6, 2},
      {5, 42, 9},
      {25, 0, 0},
      {25, 1, 1},
      {25, 4, 1},
      {25, 5, 1},
      {25, 6, 1},
      {25, 24, 1},
      {25, 25, 1},
      {25, 26, 2},
      {25, 49, 2},
      {25, 50, 2},
      {25, 51, 3},
      {25, 249, 10},
      {25, 250, 10},
      {25, 251, 11}
    };
  }

  @ParameterizedTest
  @MethodSource({"testCombinations"})
  void test(int batchSize, int numSaves, int expectedBatchCount) {
    BatchesCollector<Save, Key, Req, Map<Key, Collection<Req>>> c = newCollector(batchSize);

    List<Save> saves =
        IntStream.range(0, numSaves)
            .mapToObj(i -> new Save(new Key("key" + i / 3), new Req("req" + i)))
            .collect(Collectors.toList());

    List<Map<Key, Collection<Req>>> collected = c.collect(saves.stream());

    assertEquals(expectedBatchCount, collected.size());

    Map<Key, Collection<Req>> current = new HashMap<>();
    Iterator<Map<Key, Collection<Req>>> collectedIter = collected.iterator();
    for (int i = 0; i < saves.size(); i++) {
      if (i > 0 && (i % batchSize) == 0) {
        Map<Key, Collection<Req>> batch = collectedIter.next();
        assertEquals(current, batch);
        current = new HashMap<>();
      }

      Save save = saves.get(i);
      current.computeIfAbsent(save.key, k -> new ArrayList<>()).add(save.req);
    }
    if (current.isEmpty()) {
      assertFalse(collectedIter.hasNext());
    } else {
      Map<Key, Collection<Req>> batch = collectedIter.next();
      assertEquals(current, batch);
    }
  }

  @Test
  void testInvalidArgs() {
    assertAll(
        () -> assertThrows(IllegalArgumentException.class, () -> newCollector(0)),
        () -> assertThrows(IllegalArgumentException.class, () -> newCollector(-1)),
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    new BatchesCollector<Save, Key, Req, Map<Key, Collection<Req>>>(
                        null, e -> e.req, collected -> collected, 42)),
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    new BatchesCollector<Save, Key, Req, Map<Key, Collection<Req>>>(
                        e -> e.key, null, collected -> collected, 42)),
        () ->
            assertThrows(
                NullPointerException.class,
                () ->
                    new BatchesCollector<Save, Key, Req, Map<Key, Collection<Req>>>(
                        e -> e.key, e -> e.req, null, 42)));
  }

  private BatchesCollector<Save, Key, Req, Map<Key, Collection<Req>>> newCollector(int batchSize) {
    return new BatchesCollector<>(e -> e.key, e -> e.req, Function.identity(), batchSize);
  }

  // Relates to a DynamoDB table-name in the use of BatchesCollector in DynamoStore.save()
  static class Key {
    final String key;

    Key(String key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equal(key, ((Key) o).key);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }
  }

  // Relates to a DynamoDB WriteRequest in the use of BatchesCollector in DynamoStore.save()
  static class Req {
    final String req;

    Req(String req) {
      this.req = req;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equal(req, ((Req) o).req);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(req);
    }
  }

  static class Save {
    final Key key;
    final Req req;

    Save(Key key, Req req) {
      this.key = key;
      this.req = req;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equal(key, ((Save) o).key) && Objects.equal(req, ((Save) o).req);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, req);
    }
  }
}
