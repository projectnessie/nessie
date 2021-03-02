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
package org.projectnessie.versioned.inmem;

import java.util.Random;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.inmem.BaseObj.BaseObjProducer;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

public class TestInMemObject {

  private final InMemStore store = new InMemStore();

  private static Stream<ValueType<?>> allTypes() {
    return ValueType.values().stream()
        // repeat 20 times, cannot mix @ParameterizedTest + @RepeatedTest
        .flatMap(t -> IntStream.range(0, 20).mapToObj(x -> t));
  }

  @ParameterizedTest
  @MethodSource("allTypes")
  void testEqualsAndHashCode(ValueType<?> type) {
    Random r = new Random();

    long seed = r.nextLong();
    BaseObj<?> inst1 = createInstance(type, new Random(seed));
    BaseObj<?> inst2 = createInstance(type, new Random(seed));

    Assertions.assertEquals(inst1, inst2);
    Assertions.assertEquals(inst1.hashCode(), inst2.hashCode());
  }

  @ParameterizedTest
  @MethodSource("allTypes")
  void testCopy(ValueType<?> type) {
    Random r = new Random();

    BaseObj<?> inst = createInstance(type, r);
    BaseObj<?> copy = inst.copy();

    Assertions.assertEquals(inst, copy);
  }

  @SuppressWarnings("unchecked")
  private <C extends BaseValue<C>, V extends BaseObj<C>> V createInstance(ValueType<C> type, Random r) {
    BaseObjProducer<C> producer = store.newProducer(type);
    SampleEntities.produceRandomTo(type, (C) producer, r);
    return (V) producer.build();
  }
}
