/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.versioned.storage.commontests;

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;

public class TestKeyIndexTestSet {
  @RepeatedTest(100)
  @Disabled("Used to perf-test key-generation, not a functional test")
  void initialize() {
    KeyIndexTestSet.IndexTestSetGenerator<CommitOp> builder =
        KeyIndexTestSet.<CommitOp>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(5)
                    .foldersPerLevel(5)
                    .tablesPerNamespace(50)
                    .deterministic(true)
                    .build())
            .elementSupplier(
                key -> indexElement(key, commitOp(CommitOp.Action.ADD, 1, randomObjId())))
            .elementSerializer(CommitOp.COMMIT_OP_SERIALIZER)
            .build();

    builder.generateIndexTestSet();
  }
}
