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
package org.projectnessie.versioned.storage.common.indexes;

import static java.util.Collections.emptySet;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.emptyImmutableIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;

import com.google.protobuf.ByteString;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.persist.ObjId;

@ExtendWith(SoftAssertionsExtension.class)
public class TestImmutableEmptyIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void immutableEmpty() {
    StoreIndex<CommitOp> index = emptyImmutableIndex(COMMIT_OP_SERIALIZER);

    CommitOp commitOp = commitOp(ADD, 0, ObjId.randomObjId());

    soft.assertThat(index.elementCount()).isEqualTo(0);
    soft.assertThat(index.isLoaded()).isTrue();
    soft.assertThat(index.isModified()).isFalse();
    soft.assertThat(index.first()).isNull();
    soft.assertThat(index.last()).isNull();
    soft.assertThat(index.estimatedSerializedSize()).isEqualTo(1);
    soft.assertThat(index.serialize()).isEqualTo(ByteString.copyFrom(new byte[] {(byte) 1}));
    soft.assertThat(index.asKeyList()).isEmpty();
    soft.assertThat(index.stripes()).isEmpty();
    soft.assertThatThrownBy(() -> index.add(indexElement(key("foo"), commitOp)))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> index.remove(key("foo")))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThat(index.get(key("foo"))).isNull();
    soft.assertThat(index.contains(key("foo"))).isFalse();
    soft.assertThatCode(() -> index.updateAll(e -> commitOp)).doesNotThrowAnyException();
    soft.assertThat(index.iterator(null, null, false)).isExhausted();
  }

  @Test
  public void stateRelated() {
    StoreIndex<CommitOp> index = emptyImmutableIndex(COMMIT_OP_SERIALIZER);

    soft.assertThat(index.asMutableIndex()).isNotSameAs(index);
    soft.assertThat(index.loadIfNecessary(emptySet())).isSameAs(index);
    soft.assertThat(index.isMutable()).isFalse();
    soft.assertThatThrownBy(() -> index.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
