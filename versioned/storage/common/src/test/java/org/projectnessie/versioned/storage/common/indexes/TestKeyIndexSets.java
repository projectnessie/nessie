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
package org.projectnessie.versioned.storage.common.indexes;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.commontests.ImmutableRealisticKeySet;
import org.projectnessie.versioned.storage.commontests.KeyIndexTestSet;

@ExtendWith(SoftAssertionsExtension.class)
public class TestKeyIndexSets {
  @InjectSoftAssertions SoftAssertions soft;

  @ParameterizedTest
  @MethodSource("keyIndexSetConfigs")
  @Timeout(30) // if this test hits the timeout, then that's a legit bug !!
  void keyIndexSetTests(
      int namespaceLevels, int foldersPerLevel, int tablesPerNamespace, boolean deterministic) {

    KeyIndexTestSet.IndexTestSetGenerator<CommitOp> builder =
        KeyIndexTestSet.<CommitOp>newGenerator()
            .keySet(
                ImmutableRealisticKeySet.builder()
                    .namespaceLevels(namespaceLevels)
                    .foldersPerLevel(foldersPerLevel)
                    .tablesPerNamespace(tablesPerNamespace)
                    .deterministic(deterministic)
                    .build())
            .elementSupplier(
                key -> indexElement(key, commitOp(CommitOp.Action.ADD, 1, randomObjId())))
            .elementSerializer(COMMIT_OP_SERIALIZER)
            .build();

    KeyIndexTestSet<CommitOp> keyIndexTestSet = builder.generateIndexTestSet();

    soft.assertThatCode(keyIndexTestSet::serialize).doesNotThrowAnyException();
    soft.assertThatCode(keyIndexTestSet::deserialize).doesNotThrowAnyException();
    soft.assertThat(
            ((StoreIndexImpl<CommitOp>) keyIndexTestSet.deserialize()).setModified().serialize())
        .isEqualTo(keyIndexTestSet.serialized());
    soft.assertThatCode(keyIndexTestSet::randomGetKey).doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              StoreIndex<CommitOp> deserialized = keyIndexTestSet.deserialize();
              deserialized.add(
                  indexElement(key("zzzzzzz", "key"), commitOp(ADD, 1, randomObjId())));
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
    soft.assertThatCode(
            () -> {
              StoreIndex<CommitOp> deserialized = keyIndexTestSet.deserialize();
              for (char c = 'a'; c <= 'z'; c++) {
                deserialized.add(
                    indexElement(key(c + "x", "key"), commitOp(ADD, 1, randomObjId())));
              }
              deserialized.serialize();
            })
        .doesNotThrowAnyException();
  }

  static Stream<Arguments> keyIndexSetConfigs() {
    return Stream.of(
        arguments(2, 2, 5, true),
        arguments(2, 2, 5, false),
        arguments(2, 2, 20, true),
        arguments(2, 2, 20, false),
        arguments(5, 5, 50, true),
        arguments(5, 5, 50, false));
  }
}
