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
package org.projectnessie.versioned.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.Put;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.StringSerializer;
import org.projectnessie.versioned.impl.condition.ConditionExpression;
import org.projectnessie.versioned.impl.condition.UpdateExpression;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.LoadStep;
import org.projectnessie.versioned.store.NotFoundException;
import org.projectnessie.versioned.store.SaveOp;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

public class TestExceptionHandling {
  @Test
  void testCheckedException() {
    // Checked, unhandled exceptions are wrapped in a RE
    assertEquals("Failure during testCheckedException",
        assertThrows(RuntimeException.class,
            () -> {
              throw TieredVersionStore.unhandledException("testCheckedException", new Exception("foo"));
            }).getMessage());
  }

  @Test
  void testUncheckedException() {
    // Unchecked exceptions are just rethrown
    assertEquals("foo",
        assertThrows(RuntimeException.class,
            () -> {
              throw TieredVersionStore.unhandledException("testUncheckedException", new RuntimeException("foo"));
            }).getMessage());
    assertEquals("bar",
        assertThrows(Error.class,
            () -> {
              throw TieredVersionStore.unhandledException("testUncheckedException", new Error("bar"));
            }).getMessage());
  }

  @Test
  void testTvsCommit() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().commit(BranchName.of("branch"), Optional.empty(), "foo", Collections.singletonList(Put.of(Key.of("key"), "bnar")))
            ).getMessage());
  }

  @Test
  void testTvsToHash() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().toHash(BranchName.of("branch"))
            ).getMessage());
  }

  @Test
  @Disabled("GC not yet implemented")
  void testTvsCollectGarbage() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            tvs()::collectGarbage
            ).getMessage());
  }

  @Test
  void testTvsDelete() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().delete(BranchName.of("branch"), Optional.empty())
            ).getMessage());
  }

  @Test
  void testTvsCreate() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().create(BranchName.of("branch"), Optional.empty())
            ).getMessage());
  }

  @Test
  void testTvsGetCommits() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().getCommits(BranchName.of("branch"))
            ).getMessage());
  }

  @Test
  void testTvsGetDiffs() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().getDiffs(BranchName.of("branch"), BranchName.of("branch2"))
            ).getMessage());
  }

  @Test
  void testTvsGetKeys() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().getKeys(BranchName.of("branch"))
            ).getMessage());
  }

  @Test
  void testTvsGetValues() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().getValues(BranchName.of("branch"), Collections.singletonList(Key.of("key")))
            ).getMessage());
  }

  @Test
  void testTvsGetValue() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().getValue(BranchName.of("branch"), Key.of("key"))
            ).getMessage());
  }

  @Test
  void testTvsToRef() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            () -> tvs().toRef("branch")
            ).getMessage());
  }

  @Test
  void testTvsGetNamedRefs() {
    assertEquals("throwing store",
        assertThrows(RuntimeException.class,
            tvs()::getNamedRefs
            ).getMessage());
  }

  private TieredVersionStore<String, String> tvs() {
    StoreWorker<String, String> worker = StoreWorker.of(StringSerializer.getInstance(), StringSerializer.getInstance());
    return new TieredVersionStore<>(worker, new ThrowingStore(), false);
  }

  private static class ThrowingStore implements Store {
    @Override
    public void start() {
      throw new RuntimeException("throwing store");
    }

    @Override
    public void close() {
      throw new RuntimeException("throwing store");
    }

    @Override
    public void load(LoadStep loadstep) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> boolean putIfAbsent(SaveOp<C> saveOp) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> void put(SaveOp<C> saveOp,
        Optional<ConditionExpression> condition) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> boolean delete(ValueType<C> type, Id id,
        Optional<ConditionExpression> condition) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public void save(List<SaveOp<?>> ops) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> void loadSingle(ValueType<C> type, Id id, C consumer) {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> boolean update(ValueType<C> type, Id id,
        UpdateExpression update, Optional<ConditionExpression> condition,
        Optional<BaseValue<C>> consumer) throws NotFoundException {
      throw new RuntimeException("throwing store");
    }

    @Override
    public <C extends BaseValue<C>> Stream<Acceptor<C>> getValues(ValueType<C> type) {
      throw new RuntimeException("throwing store");
    }
  }
}
