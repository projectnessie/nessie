/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.versioned.storage.jdbc2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.common.persist.Reference.reference;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.commontests.AbstractPersistTests;
import org.projectnessie.versioned.storage.commontests.objtypes.SimpleTestObj;
import org.projectnessie.versioned.storage.jdbc2tests.MssqlBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@NessieBackend(MssqlBackendTestFactory.class)
public class ITMssqlPersist extends AbstractPersistTests {

  @Nested
  @ExtendWith(PersistExtension.class)
  class SqlServerContracts {
    @NessiePersist Persist persist;

    @Test
    void referenceNamesAreCaseSensitive() throws Exception {
      var foo = reference("Foo", randomObjId(), false, 1L, null);
      var fooLower = reference("foo", randomObjId(), false, 1L, null);
      assertEquals(foo, persist.addReference(foo));
      assertEquals(fooLower, persist.addReference(fooLower));
      assertEquals(foo, persist.fetchReference("Foo"));
      assertEquals(fooLower, persist.fetchReference("foo"));
    }

    @Test
    void concurrentStoreObjDoesNotThrow() throws Exception {
      var obj = SimpleTestObj.builder().id(randomObjId()).text("same").build();
      ExecutorService pool = Executors.newFixedThreadPool(2);
      try {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        tasks.add(() -> persist.storeObj(obj));
        tasks.add(() -> persist.storeObj(obj));
        List<Future<Boolean>> futures = pool.invokeAll(tasks);
        boolean first = futures.get(0).get();
        boolean second = futures.get(1).get();
        assertTrue(first ^ second);
        assertFalse(first && second);
      } finally {
        pool.shutdownNow();
      }
    }
  }
}
