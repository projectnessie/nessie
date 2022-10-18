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

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.storage.common.indexes.IndexLoader.notLoading;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexElement.indexElement;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.deserializeStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.indexFromSplits;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.indexFromStripes;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.lazyStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.commitOp;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.commontests.KeyIndexTestSet.basicIndexTestSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.commontests.KeyIndexTestSet;

@ExtendWith(SoftAssertionsExtension.class)
public class TestStripedIndexImpl {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void isLoadedReflectedLazy() {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    List<StoreIndex<CommitOp>> originalStripesList = reference.divide(5);
    Supplier<List<StoreIndex<CommitOp>>> stripesSupplier =
        () ->
            originalStripesList.stream()
                .map(s -> deserializeStoreIndex(s.serialize(), COMMIT_OP_SERIALIZER, (k, v) -> v))
                .collect(Collectors.toList());
    List<StoreKey> firstLastKeys =
        stripesSupplier.get().stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<StoreIndex<CommitOp>> stripedSupplier;
    stripedSupplier =
        () -> {
          List<StoreIndex<CommitOp>> originalStripes = stripesSupplier.get();
          List<StoreIndex<CommitOp>> stripes =
              originalStripes.stream()
                  .map(s -> lazyStoreIndex(() -> s))
                  .collect(Collectors.toList());
          return indexFromSplits(
              stripes,
              firstLastKeys,
              indexes -> {
                @SuppressWarnings("unchecked")
                StoreIndex<CommitOp>[] r = new StoreIndex[indexes.length];
                // Use reference equality in this test to identify the stripe to be "loaded"
                for (StoreIndex<CommitOp> index : indexes) {
                  for (int i = 0; i < stripes.size(); i++) {
                    StoreIndex<CommitOp> lazyStripe = stripes.get(i);
                    if (lazyStripe == index) {
                      r[i] = originalStripes.get(i);
                    }
                  }
                }
                return r;
              });
        };

    StoreIndex<CommitOp> striped = stripedSupplier.get();
    soft.assertThat(striped.isLoaded()).isFalse();

    for (StoreKey key : firstLastKeys) {
      striped = stripedSupplier.get();
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.contains(key)).isTrue();
      soft.assertThat(striped.isLoaded()).isTrue();
    }

    for (StoreIndex<CommitOp> s : stripesSupplier.get()) {
      striped = stripedSupplier.get();
      StoreKey key = s.asKeyList().get(1);
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.contains(key)).isTrue();
      soft.assertThat(striped.isLoaded()).isTrue();

      striped = stripedSupplier.get();
      soft.assertThat(striped.isLoaded()).isFalse();
      soft.assertThat(striped.get(key)).isNotNull();
      soft.assertThat(striped.isLoaded()).isTrue();
    }
  }

  @Test
  public void isLoadedReflectedEager() {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    List<StoreIndex<CommitOp>> originalStripes = reference.divide(5);
    List<StoreKey> firstLastKeys =
        originalStripes.stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<StoreIndex<CommitOp>> stripedSupplier;

    List<StoreIndex<CommitOp>> stripes;
    stripes = originalStripes;
    stripedSupplier = () -> indexFromSplits(stripes, firstLastKeys, notLoading());

    StoreIndex<CommitOp> striped = stripedSupplier.get();
    soft.assertThat(striped.isLoaded()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void isModifiedReflected(boolean lazyStripes) {
    StoreIndex<CommitOp> reference = KeyIndexTestSet.basicIndexTestSet().keyIndex();

    List<StoreIndex<CommitOp>> originalStripesList = reference.divide(5);
    Supplier<List<StoreIndex<CommitOp>>> stripesSupplier =
        () ->
            originalStripesList.stream()
                .map(s -> deserializeStoreIndex(s.serialize(), COMMIT_OP_SERIALIZER, (k, v) -> v))
                .collect(Collectors.toList());
    List<StoreKey> firstLastKeys =
        stripesSupplier.get().stream()
            .flatMap(s -> Stream.of(s.first(), s.last()))
            .collect(Collectors.toList());

    Supplier<StoreIndex<CommitOp>> stripedSupplier;
    if (lazyStripes) {
      stripedSupplier =
          () -> {
            List<StoreIndex<CommitOp>> originalStripes = stripesSupplier.get();
            List<StoreIndex<CommitOp>> stripes =
                originalStripes.stream()
                    .map(s -> lazyStoreIndex(() -> s))
                    .collect(Collectors.toList());
            return indexFromSplits(
                stripes,
                firstLastKeys,
                indexes -> {
                  @SuppressWarnings("unchecked")
                  StoreIndex<CommitOp>[] r = new StoreIndex[indexes.length];
                  // Use reference equality in this test to identify the stripe to be "loaded"
                  for (StoreIndex<CommitOp> index : indexes) {
                    for (int i = 0; i < stripes.size(); i++) {
                      StoreIndex<CommitOp> lazyStripe = stripes.get(i);
                      if (lazyStripe == index) {
                        r[i] = originalStripes.get(i);
                      }
                    }
                  }
                  return r;
                });
          };
    } else {
      stripedSupplier = () -> indexFromSplits(stripesSupplier.get(), firstLastKeys, notLoading());
    }

    StoreIndex<CommitOp> striped = stripedSupplier.get();
    soft.assertThat(striped.isModified()).isFalse();

    for (StoreKey key : firstLastKeys) {
      striped = stripedSupplier.get();
      soft.assertThat(striped.isModified()).isFalse();
      striped.add(indexElement(key, commitOp(ADD, 1, randomObjId())));
      soft.assertThat(striped.isModified()).isTrue();
    }

    for (StoreIndex<CommitOp> s : stripesSupplier.get()) {
      striped = stripedSupplier.get();
      StoreKey key = s.asKeyList().get(1);
      soft.assertThat(striped.isModified()).isFalse();
      striped.add(indexElement(key, commitOp(ADD, 1, randomObjId())));
      soft.assertThat(striped.isModified()).isTrue();
    }
  }

  @SuppressWarnings("ConstantConditions")
  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  public void stripedLazy(int numStripes) {
    KeyIndexTestSet<CommitOp> indexTestSet = basicIndexTestSet();

    StoreIndex<CommitOp> striped = indexFromStripes(indexTestSet.keyIndex().divide(numStripes));
    List<StoreIndex<CommitOp>> stripes = striped.stripes();

    // Sanity checks
    soft.assertThat(stripes).hasSize(numStripes);

    boolean[] individualLoads = new boolean[numStripes];
    boolean[] bulkLoads = new boolean[numStripes];

    List<StoreKey> firstLastKeys =
        stripes.stream().flatMap(s -> Stream.of(s.first(), s.last())).collect(Collectors.toList());

    // This supplier provides a striped-over-lazy-segments index. Individually loaded stripes
    // are marked in 'individualLoads' and bulk-loaded stripes in 'bulkLoads'.
    Supplier<StoreIndex<CommitOp>> lazyIndexSupplier =
        () -> {
          Arrays.fill(bulkLoads, false);
          Arrays.fill(individualLoads, false);

          List<StoreIndex<CommitOp>> lazyStripes = new ArrayList<>(stripes.size());
          for (int i = 0; i < stripes.size(); i++) {
            StoreIndex<CommitOp> stripe = stripes.get(i);
            int index = i;
            lazyStripes.add(
                StoreIndexes.lazyStoreIndex(
                    () -> {
                      individualLoads[index] = true;
                      return stripe;
                    }));
          }

          return indexFromSplits(
              lazyStripes,
              firstLastKeys,
              indexes -> {
                @SuppressWarnings("unchecked")
                StoreIndex<CommitOp>[] r = new StoreIndex[indexes.length];
                for (int i = 0; i < indexes.length; i++) {
                  if (indexes[i] != null) {
                    bulkLoads[i] = true;
                    r[i] = stripes.get(i);
                  }
                }
                return r;
              });
        };

    for (int i = 0; i < stripes.size(); i++) {
      StoreIndex<CommitOp> refStripe = stripes.get(i);

      boolean[] expectLoaded = new boolean[numStripes];
      expectLoaded[i] = true;

      StoreIndex<CommitOp> lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(singleton(refStripe.first()));
      soft.assertThat(lazyStripedIndex.contains(refStripe.first())).isTrue();
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(singleton(refStripe.last()));
      soft.assertThat(lazyStripedIndex.contains(refStripe.last())).isTrue();
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(singleton(refStripe.first()));
      soft.assertThat(lazyStripedIndex.get(refStripe.first()))
          .extracting(StoreIndexElement::key)
          .isEqualTo(refStripe.first());
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(singleton(refStripe.last()));
      soft.assertThat(lazyStripedIndex.get(refStripe.last()))
          .extracting(StoreIndexElement::key)
          .isEqualTo(refStripe.last());
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // A key before the first stripe's first key does NOT fire a load
    {
      boolean[] expectLoaded = new boolean[numStripes];
      StoreIndex<CommitOp> lazyStripedIndex = lazyIndexSupplier.get();
      StoreKey key = keyFromString("");
      lazyStripedIndex.loadIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.contains(key);
      lazyStripedIndex.get(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // A key after the last stripe's last key does NOT fire a load
    {
      boolean[] expectLoaded = new boolean[numStripes];
      StoreIndex<CommitOp> lazyStripedIndex = lazyIndexSupplier.get();
      StoreKey key = keyFromString("þZZZZ");
      lazyStripedIndex.loadIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.contains(key);
      lazyStripedIndex.get(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }

    // Keys "between" stripes must NOT fire a load
    for (int i = 0; i < stripes.size(); i++) {
      StoreIndex<CommitOp> stripe = stripes.get(i);
      boolean[] expectLoaded = new boolean[numStripes];

      // Any key before between two stripes must not fire a load
      StoreIndex<CommitOp> lazyStripedIndex = lazyIndexSupplier.get();
      StoreKey key = keyFromString(stripe.last().rawString() + "AA");
      lazyStripedIndex.loadIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // check contains() + get()
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.contains(key);
      lazyStripedIndex.get(key);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // Any key in a stripe must fire a load
      expectLoaded[i] = true;
      lazyStripedIndex = lazyIndexSupplier.get();
      key = keyFromString(stripe.first().rawString() + "AA");
      lazyStripedIndex.loadIfNecessary(singleton(key));
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      // check contains() + get()
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.contains(key);
      soft.assertThat(bulkLoads).containsOnly(false);
      soft.assertThat(individualLoads).containsExactly(expectLoaded);
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.get(key);
      soft.assertThat(bulkLoads).containsOnly(false);
      soft.assertThat(individualLoads).containsExactly(expectLoaded);
    }

    {
      boolean[] expectLoaded = new boolean[numStripes];
      Arrays.fill(expectLoaded, true);

      Set<StoreKey> allFirstKeys =
          stripes.stream().map(StoreIndex::first).collect(Collectors.toSet());
      StoreIndex<CommitOp> lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(allFirstKeys);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);

      Set<StoreKey> allLastKeys =
          stripes.stream().map(StoreIndex::last).collect(Collectors.toSet());
      lazyStripedIndex = lazyIndexSupplier.get();
      lazyStripedIndex.loadIfNecessary(allLastKeys);
      soft.assertThat(bulkLoads).containsExactly(expectLoaded);
      soft.assertThat(individualLoads).containsOnly(false);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
  public void striped(int numStripes) {
    KeyIndexTestSet<CommitOp> indexTestSet = basicIndexTestSet();

    StoreIndex<CommitOp> source = indexTestSet.keyIndex();
    StoreIndex<CommitOp> striped = indexFromStripes(indexTestSet.keyIndex().divide(numStripes));

    // Sanity checks
    soft.assertThat(striped.stripes()).hasSize(numStripes);

    soft.assertThat(striped.elementCount()).isEqualTo(source.elementCount());
    soft.assertThat(striped.asKeyList()).containsExactlyElementsOf(source.asKeyList());
    soft.assertThat(striped.first()).isEqualTo(source.first());
    soft.assertThat(striped.last()).isEqualTo(source.last());
    soft.assertThat(striped).containsExactlyElementsOf(source);
    soft.assertThat(newArrayList(striped.iterator()))
        .isNotEmpty()
        .containsExactlyElementsOf(newArrayList(source.iterator()));

    soft.assertThat(striped.estimatedSerializedSize())
        .isEqualTo(striped.stripes().stream().mapToInt(StoreIndex::estimatedSerializedSize).sum());
    soft.assertThatThrownBy(striped::serialize).isInstanceOf(UnsupportedOperationException.class);
    soft.assertThatThrownBy(() -> striped.updateAll(el -> null))
        .isInstanceOf(UnsupportedOperationException.class);

    for (StoreKey key : indexTestSet.keys()) {
      soft.assertThat(striped.contains(key)).isTrue();
      soft.assertThat(striped.contains(keyFromString(key.rawString() + "xyz"))).isFalse();
      soft.assertThat(striped.get(key)).isNotNull();
      soft.assertThat(newArrayList(striped.iterator(key, key, false)))
          .containsExactlyElementsOf(newArrayList(source.iterator(key, key, false)));
      soft.assertThat(newArrayList(striped.iterator(key, null, false)))
          .isNotEmpty()
          .containsExactlyElementsOf(newArrayList(source.iterator(key, null, false)));
      soft.assertThat(newArrayList(striped.iterator(null, key, false)))
          .isNotEmpty()
          .containsExactlyElementsOf(newArrayList(source.iterator(null, key, false)));
    }

    StoreIndex<CommitOp> stripedFromStripes = indexFromStripes(striped.stripes());
    soft.assertThat(newArrayList(stripedFromStripes))
        .containsExactlyElementsOf(newArrayList(striped));
    soft.assertThat(stripedFromStripes)
        .asInstanceOf(type(StoreIndex.class))
        .extracting(StoreIndex::stripes, StoreIndex::elementCount, StoreIndex::asKeyList)
        .containsExactly(striped.stripes(), striped.elementCount(), striped.asKeyList());
  }

  @Test
  public void stateRelated() {
    KeyIndexTestSet<CommitOp> indexTestSet = basicIndexTestSet();
    StoreIndex<CommitOp> striped = indexFromStripes(indexTestSet.keyIndex().divide(3));

    soft.assertThat(striped.asMutableIndex()).isSameAs(striped);
    soft.assertThat(striped.loadIfNecessary(emptySet())).isSameAs(striped);
    soft.assertThat(striped.isMutable()).isTrue();
    soft.assertThatThrownBy(() -> striped.divide(3))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void modifyingStripedRemoveIterative(boolean lazy) {
    KeyIndexTestSet<CommitOp> indexTestSet = basicIndexTestSet();
    StoreIndex<CommitOp> source = indexTestSet.keyIndex();

    StoreIndex<CommitOp> striped = indexFromStripes(indexTestSet.keyIndex().divide(3));
    if (lazy) {
      List<StoreIndex<CommitOp>> lazyStripes =
          striped.stripes().stream().map(i -> lazyStoreIndex(() -> i)).collect(Collectors.toList());
      striped = indexFromStripes(lazyStripes);
    }

    List<StoreKey> keyList = source.asKeyList();
    int expectedElementCount = source.elementCount();

    while (!keyList.isEmpty()) {
      StoreKey key = keyList.get(0);
      source.remove(key);
      striped.remove(key);
      expectedElementCount--;

      soft.assertThat(stream(spliteratorUnknownSize(striped.iterator(), 0), false))
          .containsExactlyElementsOf(newArrayList(source));
      soft.assertThat(striped.elementCount())
          .isEqualTo(source.elementCount())
          .isEqualTo(expectedElementCount);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void modifyingStripedAdding(boolean lazy) {
    KeyIndexTestSet<CommitOp> indexTestSet = basicIndexTestSet();
    StoreIndex<CommitOp> source = indexTestSet.keyIndex();

    List<StoreIndexElement<CommitOp>> elements = newArrayList(source.iterator());

    StoreIndex<CommitOp> indexEven = newStoreIndex(COMMIT_OP_SERIALIZER);
    StoreIndex<CommitOp> indexOdd = newStoreIndex(COMMIT_OP_SERIALIZER);

    for (int i = 0; i < elements.size(); i += 2) {
      indexEven.add(elements.get(i));
    }
    for (int i = 1; i < elements.size(); i += 2) {
      indexOdd.add(elements.get(i));
    }

    StoreIndex<CommitOp> striped = indexFromStripes(indexEven.divide(4));
    if (lazy) {
      List<StoreIndex<CommitOp>> lazyStripes =
          striped.stripes().stream().map(i -> lazyStoreIndex(() -> i)).collect(Collectors.toList());
      striped = indexFromStripes(lazyStripes);
    }

    soft.assertThat(newArrayList(striped)).containsExactlyElementsOf(newArrayList(indexEven));
    soft.assertThat(striped.elementCount())
        .isEqualTo(source.elementCount() / 2)
        .isEqualTo(elements.size() / 2);

    indexOdd.forEach(striped::add);

    soft.assertThat(newArrayList(striped)).containsExactlyElementsOf(newArrayList(source));
    soft.assertThat(striped.elementCount())
        .isEqualTo(source.elementCount())
        .isEqualTo(elements.size());
  }
}
