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
package org.projectnessie.versioned.tiered.nontx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.versioned.tiered.adapter.KeyWithBytes;
import org.projectnessie.versioned.tiered.tests.AbstractTestSerialization;

class TestSerializationNonTx extends AbstractTestSerialization {
  @BeforeAll
  static void setupParams() {
    params =
        () ->
            paramsUntyped()
                .flatMap(
                    ser ->
                        Stream.of(
                            ser.withType(
                                TestSerializationNonTx::createPointer, GlobalStatePointer.class),
                            ser.withType(
                                TestSerializationNonTx::createGlobalLog,
                                GlobalStateLogEntry.class)));
  }

  static GlobalStatePointer createPointer() {
    List<Ref> refs = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      refs.add(Ref.of(randomHash(), randomRef()));
    }
    return GlobalStatePointer.of(randomHash(), refs);
  }

  static GlobalStateLogEntry createGlobalLog() {
    List<KeyWithBytes> puts = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      puts.add(KeyWithBytes.of(randomKey(), (byte) 0, randomBytes(120)));
    }
    return GlobalStateLogEntry.of(
        System.nanoTime() / 1000L,
        randomHash(),
        Arrays.asList(
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash(),
            randomHash()),
        puts);
  }
}
