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

import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.RepeatedTest;
import org.projectnessie.versioned.impl.SampleEntities;
import org.projectnessie.versioned.inmem.BaseObj.BaseObjProducer;
import org.projectnessie.versioned.store.ValueType;
import org.projectnessie.versioned.tiered.BaseValue;

public class TestObjectEquals {

  private final InMemStore store = new InMemStore();

  @RepeatedTest(1000)
  void testEqualsAndHashCode() {
    Random r = new Random();

    ValueType<?> type = randomType(r);

    long seed = r.nextLong();
    BaseObj<?> inst1 = createInstance(type, new Random(seed));
    BaseObj<?> inst2 = createInstance(type, new Random(seed));

    Assertions.assertEquals(inst1, inst2);
    Assertions.assertEquals(inst1.hashCode(), inst2.hashCode());
  }

  private ValueType<?> randomType(Random r) {
    List<ValueType<?>> types = ValueType.values();
    int typeIndex = r.nextInt(types.size());
    return types.get(typeIndex);
  }

  @SuppressWarnings("unchecked")
  private <C extends BaseValue<C>, V extends BaseObj<C>> V createInstance(ValueType<C> type, Random r) {
    BaseObjProducer<C> producer = store.newProducer(type);
    SampleEntities.produceRandomTo(type, (C) producer, r);
    return (V) producer.build();
  }
}
