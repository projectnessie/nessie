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
import static org.assertj.core.util.Lists.newArrayList;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.deserializeStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.layeredIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.lazyStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.NONE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action;
import org.projectnessie.versioned.storage.commontests.KeyIndexTestSet;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLayeredIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void isModifiedReflected() {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();
    soft.assertThat(reference.isModified()).isFalse();

    StoreIndex<CommitOp> updates = newStoreIndex(COMMIT_OP_SERIALIZER);
    for (char c = 'a'; c <= 'z'; c++) {
      updates.add(indexElement(key(c + "foo"), commitOp(ADD, 0, null)));
    }
    StoreIndex<CommitOp> layered = layeredIndex(reference, updates);
    soft.assertThat(updates.isModified()).isTrue();
    soft.assertThat(layered.isModified()).isTrue();

    updates = deserializeStoreIndex(updates.serialize(), COMMIT_OP_SERIALIZER);
    layered = layeredIndex(reference, updates);
    soft.assertThat(updates.isModified()).isFalse();
    soft.assertThat(layered.isModified()).isFalse();

    reference.add(indexElement(key("foobar"), commitOp(ADD, 0, null)));
    soft.assertThat(reference.isModified()).isTrue();
    soft.assertThat(updates.isModified()).isFalse();
    soft.assertThat(layered.isModified()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void isLoadedReflected(boolean updateReference) {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();
    StoreIndex<CommitOp> lazyReference = lazyStoreIndex(() -> reference);
    soft.assertThat(lazyReference.isLoaded()).isFalse();

    StoreIndex<CommitOp> updates = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> lazyUpdates = lazyStoreIndex(() -> updates);
    soft.assertThat(lazyUpdates.isLoaded()).isFalse();

    StoreIndex<CommitOp> layered = layeredIndex(lazyReference, lazyUpdates);
    soft.assertThat(layered.isLoaded()).isFalse();

    if (updateReference) {
      lazyReference.add(indexElement(key("abc"), commitOp(ADD, 0, null)));
      soft.assertThat(lazyReference.isLoaded()).isTrue();
      soft.assertThat(lazyUpdates.isLoaded()).isFalse();
    } else {
      lazyUpdates.add(indexElement(key("abc"), commitOp(ADD, 0, null)));
      soft.assertThat(lazyReference.isLoaded()).isFalse();
      soft.assertThat(lazyUpdates.isLoaded()).isTrue();
    }
  }

  @Test
  public void basicLayered() {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();
    reference.updateAll(el -> commitOp(NONE, el.content().payload(), el.content().value()));

    List<StoreIndexElement<CommitOp>> expected = new ArrayList<>();
    StoreIndex<CommitOp> updates = newStoreIndex(COMMIT_OP_SERIALIZER);
    for (StoreIndexElement<CommitOp> el : reference) {
      if ((expected.size() % 5) == 0) {
        el = indexElement(el.key(), commitOp(ADD, el.content().payload(), el.content().value()));
        updates.add(el);
      }
      expected.add(el);
    }

    StoreIndex<CommitOp> layered = layeredIndex(reference, updates);
    soft.assertThat(newArrayList(layered)).containsExactlyElementsOf(expected);
    soft.assertThat(layered.asKeyList()).containsExactlyElementsOf(reference.asKeyList());
    soft.assertThat(layered.elementCount()).isEqualTo(reference.elementCount());

    soft.assertThat(layered.stripes()).containsExactly(layered);

    StoreKey referenceFirst = reference.first();
    StoreKey referenceLast = reference.last();
    soft.assertThat(referenceFirst).isNotNull();
    soft.assertThat(referenceLast).isNotNull();
    soft.assertThat(layered.first()).isEqualTo(referenceFirst);
    soft.assertThat(layered.last()).isEqualTo(referenceLast);

    for (int i = 0; i < expected.size(); i++) {
      StoreIndexElement<CommitOp> el = expected.get(i);

      soft.assertThat(layered.contains(el.key())).isTrue();
      soft.assertThat(layered.get(el.key())).isEqualTo(el);
      soft.assertThat(newArrayList(layered.iterator(el.key(), el.key(), false)))
          .allMatch(elem -> elem.key().startsWith(el.key()));

      soft.assertThat(newArrayList(layered.iterator(el.key(), null, false)))
          .containsExactlyElementsOf(expected.subList(i, expected.size()));
      soft.assertThat(newArrayList(layered.iterator(null, el.key(), false)))
          .containsExactlyElementsOf(expected.subList(0, i + 1));
    }

    StoreIndexElement<CommitOp> veryFirst =
        indexElement(key("aaaaaaaaaa"), commitOp(Action.REMOVE, 1, randomObjId()));
    updates.add(veryFirst);

    soft.assertThat(layered.contains(veryFirst.key())).isTrue();
    soft.assertThat(layered.get(veryFirst.key())).isEqualTo(veryFirst);
    expected.add(0, veryFirst);
    soft.assertThat(newArrayList(layered)).containsExactlyElementsOf(expected);
    soft.assertThat(layered.elementCount()).isEqualTo(reference.elementCount() + 1);

    soft.assertThat(layered.first()).isEqualTo(veryFirst.key());
    soft.assertThat(layered.last()).isEqualTo(referenceLast);

    StoreIndexElement<CommitOp> veryLast =
        indexElement(key("zzzzzzzzz"), commitOp(Action.REMOVE, 1, randomObjId()));
    updates.add(veryLast);

    soft.assertThat(layered.contains(veryLast.key())).isTrue();
    soft.assertThat(layered.get(veryLast.key())).isEqualTo(veryLast);
    expected.add(veryLast);
    soft.assertThat(newArrayList(layered)).containsExactlyElementsOf(expected);
    soft.assertThat(layered.elementCount()).isEqualTo(reference.elementCount() + 2);

    soft.assertThat(layered.first()).isEqualTo(veryFirst.key());
    soft.assertThat(layered.last()).isEqualTo(veryLast.key());
  }

  @Test
  public void firstLastEstimated() {
    StoreIndex<CommitOp> index1 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index2 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index3 = newStoreIndex(COMMIT_OP_SERIALIZER);
    index1.add(indexElement(key("aaa"), commitOp(ADD, 0, randomObjId())));
    index2.add(indexElement(key("bbb"), commitOp(ADD, 0, randomObjId())));

    soft.assertThat(layeredIndex(index1, index2).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index2, index1).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index1, index2).last()).isEqualTo(index2.first());
    soft.assertThat(layeredIndex(index2, index1).last()).isEqualTo(index2.first());

    soft.assertThat(layeredIndex(index1, index3).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index3, index1).first()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index1, index3).last()).isEqualTo(index1.first());
    soft.assertThat(layeredIndex(index3, index1).last()).isEqualTo(index1.first());

    soft.assertThat(layeredIndex(index3, index1).estimatedSerializedSize())
        .isEqualTo(index3.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index1, index3).estimatedSerializedSize())
        .isEqualTo(index3.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index2, index1).estimatedSerializedSize())
        .isEqualTo(index2.estimatedSerializedSize() + index1.estimatedSerializedSize());
    soft.assertThat(layeredIndex(index1, index2).estimatedSerializedSize())
        .isEqualTo(index2.estimatedSerializedSize() + index1.estimatedSerializedSize());
  }

  @Test
  public void stateRelated() {
    StoreIndex<CommitOp> index1 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index2 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> layered = layeredIndex(index1, index2);

    soft.assertThatThrownBy(layered::asMutableIndex)
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThat(layered.loadIfNecessary(emptySet())).isSameAs(layered);
    soft.assertThat(layered.isMutable()).isFalse();
    soft.assertThatThrownBy(() -> layered.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void unsupported() {
    StoreIndex<CommitOp> index1 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> index2 = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> layered = layeredIndex(index1, index2);

    soft.assertThatThrownBy(layered::serialize).isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(
            () -> layered.add(indexElement(key("aaa"), commitOp(ADD, 0, randomObjId()))))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> layered.remove(key("aaa")))
        .isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> layered.updateAll(el -> null))
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
