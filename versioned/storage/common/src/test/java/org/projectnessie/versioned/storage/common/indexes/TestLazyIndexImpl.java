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
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.lazyStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.commontests.KeyIndexTestSet.basicIndexTestSet;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLazyIndexImpl {

  @InjectSoftAssertions SoftAssertions soft;

  private static StoreIndex<CommitOp> commonIndex;

  @BeforeAll
  static void setup() {
    commonIndex = basicIndexTestSet().keyIndex();
  }

  static final class Checker implements Supplier<StoreIndex<CommitOp>> {
    final AtomicInteger called = new AtomicInteger();

    @Override
    public StoreIndex<CommitOp> get() {
      called.incrementAndGet();
      return commonIndex;
    }
  }

  static final class FailChecker implements Supplier<StoreIndex<CommitOp>> {
    final AtomicInteger called = new AtomicInteger();

    @Override
    public StoreIndex<CommitOp> get() {
      called.incrementAndGet();
      throw new RuntimeException("fail check");
    }
  }

  @SuppressWarnings("ReturnValueIgnored")
  static Stream<Arguments> lazyCalls() {
    return Stream.of(
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::stripes, "stripes"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::elementCount, "elementCount"),
        arguments(
            (Consumer<StoreIndex<CommitOp>>) StoreIndex::estimatedSerializedSize,
            "estimatedSerializedSize"),
        arguments(
            (Consumer<StoreIndex<CommitOp>>)
                i -> i.add(indexElement(key("foo"), commitOp(ADD, 1, randomObjId()))),
            "add"),
        arguments((Consumer<StoreIndex<CommitOp>>) i -> i.updateAll(el -> null), "updateAll"),
        arguments((Consumer<StoreIndex<CommitOp>>) i -> i.remove(key("foo")), "remove"),
        arguments((Consumer<StoreIndex<CommitOp>>) i -> i.contains(key("foo")), "contains"),
        arguments((Consumer<StoreIndex<CommitOp>>) i -> i.get(key("foo")), "get"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::first, "first"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::last, "last"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::asKeyList, "asKeyList"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::iterator, "iterator"),
        arguments((Consumer<StoreIndex<CommitOp>>) i -> i.iterator(null, null, false), "iterator"),
        arguments((Consumer<StoreIndex<CommitOp>>) StoreIndex::serialize, "serialize"));
  }

  @ParameterizedTest
  @MethodSource("lazyCalls")
  public void calls(Consumer<StoreIndex<CommitOp>> invoker, String ignore) {
    Checker checker = new Checker();
    StoreIndex<CommitOp> lazyIndex = lazyStoreIndex(checker);

    soft.assertThat(checker.called).hasValue(0);
    invoker.accept(lazyIndex);
    soft.assertThat(checker.called).hasValue(1);
    invoker.accept(lazyIndex);
    soft.assertThat(checker.called).hasValue(1);
  }

  @ParameterizedTest
  @MethodSource("lazyCalls")
  public void fails(Consumer<StoreIndex<CommitOp>> invoker, String ignore) {
    FailChecker checker = new FailChecker();
    StoreIndex<CommitOp> lazyIndex = lazyStoreIndex(checker);

    soft.assertThat(checker.called).hasValue(0);
    soft.assertThatThrownBy(() -> invoker.accept(lazyIndex))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("fail check");
    soft.assertThat(checker.called).hasValue(1);
    soft.assertThatThrownBy(() -> invoker.accept(lazyIndex))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("fail check");
    soft.assertThat(checker.called).hasValue(1);
  }

  @Test
  public void stateRelated() {
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> lazyIndex = lazyStoreIndex(() -> index);

    soft.assertThat(lazyIndex.asMutableIndex()).isSameAs(index);
    soft.assertThat(lazyIndex.loadIfNecessary(emptySet())).isSameAs(index);
    soft.assertThat(lazyIndex.isMutable()).isFalse();
    soft.assertThatThrownBy(() -> lazyIndex.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void firstLastKeyDontLoad() {
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreKey first = key("aaa");
    StoreKey last = key("zzz");
    index.add(StoreIndexElement.indexElement(first, commitOp(ADD, 42, null)));
    index.add(StoreIndexElement.indexElement(last, commitOp(ADD, 42, null)));
    StoreIndex<CommitOp> lazyIndex = lazyStoreIndex(() -> index, first, last);

    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.first()).isEqualTo(first);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.last()).isEqualTo(last);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.contains(first)).isTrue();
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.contains(last)).isTrue();
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);
  }

  @Test
  public void firstLastKeyDoLoadIfNotSpecified() {
    StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreKey first = key("aaa");
    StoreKey last = key("zzz");
    index.add(StoreIndexElement.indexElement(first, commitOp(ADD, 42, null)));
    index.add(StoreIndexElement.indexElement(last, commitOp(ADD, 42, null)));
    StoreIndex<CommitOp> lazyIndex = lazyStoreIndex(() -> index);

    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(false, false, false);

    soft.assertThat(lazyIndex.first()).isEqualTo(first);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(true, true, false);

    lazyIndex = lazyStoreIndex(() -> index);
    soft.assertThat(lazyIndex.last()).isEqualTo(last);
    soft.assertThat(lazyIndex)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::isLoaded, StoreIndex::isModified, StoreIndex::isMutable)
        .containsExactly(true, true, false);
  }
}
