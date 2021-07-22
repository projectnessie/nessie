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
package org.projectnessie.versioned.persist.adapter.tests;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.BeforeAll;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.tests.AbstractTestSerialization;

class TestSerialization extends AbstractTestSerialization {

  @BeforeAll
  static void setupParams() {
    params =
        () ->
            paramsUntyped()
                .map(ser -> ser.withType(TestSerialization::createEntry, CommitLogEntry.class));
  }

  static CommitLogEntry createEntry() {
    return CommitLogEntry.of(
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
        randomBytes(256),
        Arrays.asList(
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32)),
            KeyWithBytes.of(randomKey(), randomId(), (byte) 1, randomBytes(32))),
        Arrays.asList(randomKey(), randomKey(), randomKey(), randomKey(), randomKey()),
        Arrays.asList(randomKey(), randomKey()),
        42,
        null,
        Collections.emptyList());
  }
}
