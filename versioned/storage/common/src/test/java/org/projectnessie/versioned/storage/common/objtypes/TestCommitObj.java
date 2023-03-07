/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.common.objtypes;

import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.objtypes.CommitHeaders.EMPTY_COMMIT_HEADERS;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCommitObj {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  public void noParent() {
    CommitObj commit =
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(1L)
            .created(42L)
            .message("msg")
            .headers(EMPTY_COMMIT_HEADERS)
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build();

    soft.assertThat(commit.directParent()).isEqualTo(EMPTY_OBJ_ID);
  }

  @Test
  public void emptyParent() {
    CommitObj commit =
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(1L)
            .created(42L)
            .addTail(EMPTY_OBJ_ID)
            .message("msg")
            .headers(EMPTY_COMMIT_HEADERS)
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build();

    soft.assertThat(commit.directParent()).isEqualTo(EMPTY_OBJ_ID);
  }

  @Test
  public void oneParent() {
    ObjId tail1 = randomObjId();
    CommitObj commit =
        CommitObj.commitBuilder()
            .id(randomObjId())
            .seq(1L)
            .created(42L)
            .addTail(tail1)
            .addTail(EMPTY_OBJ_ID)
            .message("msg")
            .headers(EMPTY_COMMIT_HEADERS)
            .incrementalIndex(emptyImmutableIndex(COMMIT_OP_SERIALIZER).serialize())
            .build();

    soft.assertThat(commit.directParent()).isEqualTo(tail1);
  }
}
