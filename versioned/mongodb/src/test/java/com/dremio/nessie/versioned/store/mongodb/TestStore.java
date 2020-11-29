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
package com.dremio.nessie.versioned.store.mongodb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.nessie.versioned.impl.Fragment;
import com.dremio.nessie.versioned.impl.InternalCommitMetadata;
import com.dremio.nessie.versioned.impl.InternalRef;
import com.dremio.nessie.versioned.impl.InternalValue;
import com.dremio.nessie.versioned.impl.L1;
import com.dremio.nessie.versioned.impl.L2;
import com.dremio.nessie.versioned.impl.L3;
import com.dremio.nessie.versioned.store.HasId;
import com.dremio.nessie.versioned.store.SaveOp;
import com.dremio.nessie.versioned.store.SimpleSchema;
import com.dremio.nessie.versioned.store.Store;
import com.dremio.nessie.versioned.store.ValueType;
import com.google.common.collect.ImmutableList;

public abstract class TestStore<S extends Store> {
  protected S store;

  /**
   * Create and start the store, if not already done.
   */
  @BeforeEach
  public void setup() {
    if (store == null) {
      this.store = createStore();
      this.store.start();
    }
  }

  /**
   * Reset the state of the store.
   */
  @AfterEach
  public void reset() {
    resetStoreState();
  }

  protected abstract S createStore();

  protected abstract void resetStoreState();

  @Test
  public void loadSingleL1() {
    putThenLoad(SampleEntities.createL1(), ValueType.L1, L1.SCHEMA);
  }

  @Test
  public void loadSingleL2() {
    putThenLoad(SampleEntities.createL2(), ValueType.L2, L2.SCHEMA);
  }

  @Test
  public void loadSingleL3() {
    putThenLoad(SampleEntities.createL3(), ValueType.L3, L3.SCHEMA);
  }

  @Test
  public void loadFragment() {
    putThenLoad(SampleEntities.createFragment(), ValueType.KEY_FRAGMENT, Fragment.SCHEMA);
  }

  @Test
  public void loadBranch() {
    putThenLoad(SampleEntities.createBranch(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void loadTag() {
    putThenLoad(SampleEntities.createTag(), ValueType.REF, InternalRef.SCHEMA);
  }

  @Test
  public void loadCommitMetadata() {
    putThenLoad(SampleEntities.createCommitMetadata(), ValueType.COMMIT_METADATA, InternalCommitMetadata.SCHEMA);
  }

  @Test
  public void loadValue() {
    putThenLoad(SampleEntities.createValue(), ValueType.VALUE, InternalValue.SCHEMA);
  }

  @Test
  public void putIfAbsentL1() {
    putIfAbsent(SampleEntities.createL1(), ValueType.L1);
  }

  @Test
  public void putIfAbsentL2() {
    putIfAbsent(SampleEntities.createL2(), ValueType.L2);
  }

  @Test
  public void putIfAbsentL3() {
    putIfAbsent(SampleEntities.createL3(), ValueType.L3);
  }

  @Test
  public void putIfAbsentFragment() {
    putIfAbsent(SampleEntities.createFragment(), ValueType.KEY_FRAGMENT);
  }

  @Test
  public void putIfAbsentBranch() {
    putIfAbsent(SampleEntities.createBranch(), ValueType.REF);
  }

  @Test
  public void putIfAbsentTag() {
    putIfAbsent(SampleEntities.createTag(), ValueType.REF);
  }

  @Test
  public void putIfAbsentCommitMetadata() {
    putIfAbsent(SampleEntities.createCommitMetadata(), ValueType.COMMIT_METADATA);
  }

  @Test
  public void putIfAbsentValue() {
    putIfAbsent(SampleEntities.createValue(), ValueType.VALUE);
  }

  @Test
  public void save() {
    final L1 l1 = SampleEntities.createL1();
    final InternalRef branch = SampleEntities.createBranch();
    final InternalRef tag = SampleEntities.createTag();
    final List<SaveOp<?>> saveOps = ImmutableList.of(
        new SaveOp<>(ValueType.L1, l1),
        new SaveOp<>(ValueType.REF, branch),
        new SaveOp<>(ValueType.REF, tag)
    );
    store.save(saveOps);

    assertEquals(
        L1.SCHEMA.itemToMap(l1, true),
        L1.SCHEMA.itemToMap(store.loadSingle(ValueType.L1, l1.getId()), true));
    assertEquals(
        InternalRef.SCHEMA.itemToMap(branch, true),
        InternalRef.SCHEMA.itemToMap(store.loadSingle(ValueType.REF, branch.getId()), true));
    assertEquals(
        InternalRef.SCHEMA.itemToMap(tag, true),
        InternalRef.SCHEMA.itemToMap(store.loadSingle(ValueType.REF, tag.getId()), true));
  }

  private <T> void putIfAbsent(T sample, ValueType type) {
    Assertions.assertTrue(store.putIfAbsent(type, sample));
    Assertions.assertFalse(store.putIfAbsent(type, sample));
  }

  private <T extends HasId> void putThenLoad(T sample, ValueType type, SimpleSchema<T> schema) {
    store.put(type, sample, Optional.empty());
    final T read = store.loadSingle(type, sample.getId());
    assertEquals(schema.itemToMap(sample, true), schema.itemToMap(read, true));
  }
}
