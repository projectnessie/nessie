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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.store.Id;
import org.projectnessie.versioned.store.Store;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.gc.IdCarrier;
import org.projectnessie.versioned.tiered.gc.IdFrame;
import org.projectnessie.versioned.tiered.gc.L1Frame;
import org.projectnessie.versioned.tiered.gc.RefFrame;

public class TestValueRetriever {

  private static final Byte DEFAULT_PAYLOAD = (byte) 0;
  private final Store store = new MockStore();

  private SparkSession spark() {
    return SparkSession.builder().appName("valueretriever").master("local[2]").getOrCreate();
  }

  @Test
  public void l1() {
    Id l21 = Id.generateRandom();
    Id l22 = Id.generateRandom();

    // empty
    store.put(InternalL1.EMPTY.toSaveOp(), Optional.empty());

    // commit 1
    InternalL1 c1 = InternalL1.EMPTY.set(1, l21);
    store.put(c1.toSaveOp(), Optional.empty());

    // commit 2
    IdMap tree2 = new IdMap(InternalL1.SIZE).withId(1, l21).withId(2, l22);
    InternalL1 c2 =
        c1.getChildWithTree(Id.generateRandom(), tree2, KeyMutationList.of(Arrays.asList()));
    store.put(c2.toSaveOp(), Optional.empty());

    // commit 3
    IdMap tree3 = new IdMap(InternalL1.SIZE).withId(2, l22);
    InternalL1 c3 =
        c2.getChildWithTree(Id.generateRandom(), tree3, KeyMutationList.of(Arrays.asList()));
    store.put(c3.toSaveOp(), Optional.empty());

    Dataset<L1Frame> dataset = L1Frame.asDataset(() -> store, spark());
    List<L1Frame> frames = dataset.collectAsList();
    Map<String, L1Frame> frameMap =
        frames.stream().collect(Collectors.toMap(l1 -> l1.getId().toString(), c -> c));

    // ensure we don't get the empty id (we inserted 3 items but the empty parent should be
    // stripped).
    assertEquals(3, frameMap.size());

    L1Frame f1 = frameMap.get(c1.getId().toString());
    assertNotNull(f1);
    assertEquals(0, f1.getParents().size());
    assertEquals(l21.toString(), f1.getChildren().get(1).toString());
    assertEquals(InternalL2.EMPTY_ID.toString(), f1.getChildren().get(2).toString());

    L1Frame f2 = frameMap.get(c2.getId().toString());
    assertNotNull(f2);
    assertEquals(1, f2.getParents().size());
    assertTrue(f2.getParents().get(0).isRecurse());

    assertEquals(l21.toString(), f2.getChildren().get(1).toString());
    assertEquals(l22.toString(), f2.getChildren().get(2).toString());

    L1Frame f3 = frameMap.get(c3.getId().toString());
    assertNotNull(f3);
    assertEquals(2, f3.getParents().size());
    assertFalse(f3.getParents().get(0).isRecurse());
    assertTrue(f3.getParents().get(1).isRecurse());
  }

  @Test
  public void ref() {
    InternalTag t1 = new InternalTag(Id.generateRandom(), "t1", Id.generateRandom(), DT.now());
    store.put(t1.toSaveOp(), Optional.empty());

    InternalBranch b1 = new InternalBranch("b1");
    store.put(b1.toSaveOp(), Optional.empty());

    InternalL1 c1 = InternalL1.EMPTY.set(1, Id.generateRandom());
    InternalBranch b2 = new InternalBranch("b2", c1);
    store.put(b2.toSaveOp(), Optional.empty());

    Map<String, RefFrame> frames =
        RefFrame.asDataset(() -> store, spark()).collectAsList().stream()
            .collect(Collectors.toMap(RefFrame::getName, Function.identity()));

    RefFrame f1 = frames.get(t1.getName());
    assertNotNull(f1);
    assertEquals(t1.getCommit().toString(), f1.getId().toString());

    RefFrame f2 = frames.get(b1.getName());
    assertNotNull(f2);
    assertEquals(b1.getLastDefinedParent().toString(), f2.getId().toString());

    RefFrame f3 = frames.get(b2.getName());
    assertNotNull(f3);
    assertEquals(b2.getLastDefinedParent().toString(), f3.getId().toString());
  }

  @Test
  public void l2() {
    Id l31 = Id.generateRandom();
    Id l32 = Id.generateRandom();
    InternalL2 l2 = InternalL2.EMPTY.set(0, l31).set(1, l32);
    store.put(l2.toSaveOp(), Optional.empty());

    IdCarrier carrier1 =
        single(
            IdCarrier.asDataset(
                ValueType.L2,
                () -> store,
                IdCarrier.L2_CONVERTER,
                Optional.of(i -> i.getId().equalsId(l2.getId())),
                spark()));
    Set<String> children =
        carrier1.getChildren().stream().map(IdFrame::toString).collect(Collectors.toSet());
    assertThat(children)
        .containsExactlyInAnyOrder(l31.toString(), l32.toString(), InternalL3.EMPTY_ID.toString());
  }

  public static boolean equals(IdFrame id1, Id id2) {
    return Arrays.equals(id1.getId(), id2.toBytes());
  }

  @Test
  public void l3() {
    Id val1 = Id.generateRandom();
    Id val2 = Id.generateRandom();
    InternalL3 l3 =
        InternalL3.EMPTY
            .set(new InternalKey(Key.of("foo")), val1, DEFAULT_PAYLOAD)
            .set(new InternalKey(Key.of("bar")), val2, DEFAULT_PAYLOAD);
    store.put(l3.toSaveOp(), Optional.empty());

    IdCarrier carrier1 =
        single(
            IdCarrier.asDataset(
                ValueType.L3,
                () -> store,
                IdCarrier.L3_CONVERTER,
                Optional.of(i -> i.getId().equalsId(l3.getId())),
                spark()));
    Set<String> children =
        carrier1.getChildren().stream().map(IdFrame::toString).collect(Collectors.toSet());
    assertThat(children).containsExactlyInAnyOrder(val1.toString(), val2.toString());
  }

  private static <T> T single(Dataset<T> dataset) {
    List<T> items = dataset.collectAsList();
    assertEquals(1, items.size());
    return items.get(0);
  }
}
