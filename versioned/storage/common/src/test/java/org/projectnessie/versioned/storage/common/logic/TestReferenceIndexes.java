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
package org.projectnessie.versioned.storage.common.logic;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.rangeClosed;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.nessie.relocated.protobuf.ByteString.copyFromUtf8;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_INCREMENTAL_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT;
import static org.projectnessie.versioned.storage.common.config.StoreConfig.CONFIG_MAX_SERIALIZED_INDEX_SIZE;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.deserializeStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreIndexes.newStoreIndex;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.key;
import static org.projectnessie.versioned.storage.common.indexes.StoreKey.keyFromString;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Add.commitAdd;
import static org.projectnessie.versioned.storage.common.logic.CreateCommit.Remove.commitRemove;
import static org.projectnessie.versioned.storage.common.logic.Logics.commitLogic;
import static org.projectnessie.versioned.storage.common.logic.Logics.indexesLogic;
import static org.projectnessie.versioned.storage.common.objtypes.CommitObj.commitBuilder;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.INCREMENTAL_ADD;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.INCREMENTAL_REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.NONE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.Action.REMOVE;
import static org.projectnessie.versioned.storage.common.objtypes.CommitOp.COMMIT_OP_SERIALIZER;
import static org.projectnessie.versioned.storage.common.objtypes.ContentValueObj.contentValue;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.COMMIT;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX;
import static org.projectnessie.versioned.storage.common.objtypes.StandardObjType.INDEX_SEGMENTS;
import static org.projectnessie.versioned.storage.common.persist.ObjId.EMPTY_OBJ_ID;
import static org.projectnessie.versioned.storage.common.persist.ObjId.randomObjId;
import static org.projectnessie.versioned.storage.commontests.AbstractCommitLogicTests.stdCommit;

import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.versioned.storage.common.exceptions.CommitConflictException;
import org.projectnessie.versioned.storage.common.exceptions.ObjNotFoundException;
import org.projectnessie.versioned.storage.common.indexes.StoreIndex;
import org.projectnessie.versioned.storage.common.indexes.StoreIndexElement;
import org.projectnessie.versioned.storage.common.indexes.StoreKey;
import org.projectnessie.versioned.storage.common.objtypes.CommitObj;
import org.projectnessie.versioned.storage.common.objtypes.CommitOp;
import org.projectnessie.versioned.storage.common.objtypes.IndexObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexSegmentsObj;
import org.projectnessie.versioned.storage.common.objtypes.IndexStripe;
import org.projectnessie.versioned.storage.common.persist.Obj;
import org.projectnessie.versioned.storage.common.persist.ObjId;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestReferenceIndexes {

  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist protected Persist persist;

  static final int REF_INDEX_LIFECYCLE_TEST_INCR_SIZE = 1024;
  // This number might need to be adjusted if/when the serialized index format changes
  static final int REF_INDEX_LIFECYCLE_TEST_CYCLES_EMBEDDED = 3;
  static final int REF_INDEX_LIFECYCLE_TEST_CYCLES_EXTERNAL = 15;
  static final int REF_INDEX_LIFECYCLE_TEST_CYCLES_EXTERNAL_RM = 9;
  static final int REF_INDEX_LIFECYCLE_TEST_SEG_SIZE = 2048;
  static final int REF_INDEX_LIFECYCLE_TEST_EMBEDDED = 4;

  /**
   * Exercises the whole possible lifecycle of a reference indexes by adding more and more keys and
   * later removing those keys.
   *
   * <p>The test verifies that the contents of the incremental index and reference indexes are
   * consistent to each other.
   */
  @Test
  public void referenceIndexLifecycle(
      @NessieStoreConfig(
              name = CONFIG_MAX_INCREMENTAL_INDEX_SIZE,
              value = "" + REF_INDEX_LIFECYCLE_TEST_INCR_SIZE)
          @NessieStoreConfig(
              name = CONFIG_MAX_SERIALIZED_INDEX_SIZE,
              value = "" + REF_INDEX_LIFECYCLE_TEST_SEG_SIZE)
          @NessieStoreConfig(
              name = CONFIG_MAX_REFERENCE_STRIPES_PER_COMMIT,
              value = "" + REF_INDEX_LIFECYCLE_TEST_EMBEDDED)
          @NessiePersist
          Persist persist)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    @SuppressWarnings("InlineMeInliner")
    String suffix200 = Strings.repeat("1234567890", 20);
    IntFunction<StoreKey> key = i -> key(format("%04x", i), suffix200);
    BiFunction<ObjId, Integer, CreateCommit> add =
        (head, num) ->
            stdCommit()
                .addAdds(commitAdd(key.apply(num), 0, EMPTY_OBJ_ID, null, null))
                .parentCommitId(head)
                .build();
    BiFunction<ObjId, Integer, CreateCommit> remove =
        (head, num) ->
            stdCommit()
                .addRemoves(commitRemove(key.apply(num), 0, EMPTY_OBJ_ID, null))
                .parentCommitId(head)
                .build();
    IntFunction<List<Obj>> contents =
        num ->
            rangeClosed(0, num & 3)
                .mapToObj(
                    i ->
                        contentValue(
                            "cid-" + num + "-" + i, 0, copyFromUtf8("val-" + num + "-" + i)))
                .collect(toList());

    int num = 0;
    ObjId head = EMPTY_OBJ_ID;
    int maxNum = REF_INDEX_LIFECYCLE_TEST_CYCLES_EXTERNAL + 10;

    // Add some keys, stop _before_ it has to spill out.
    for (; num < REF_INDEX_LIFECYCLE_TEST_CYCLES_EMBEDDED; num++) {
      head = commitWithValues(persist, commitLogic, add, contents, num, head);

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));
      soft.assertThat(commit.referenceIndex()).isNull();
      soft.assertThat(commit.referenceIndexStripes()).isEmpty();
      soft.assertThat(commit.hasReferenceIndex()).isFalse();
      refIndexVerifyKeyExistence(persist, commit, num, key);

      // "incremental remove" operations do not need to be recorded in the incremental index,
      // because there is no "shadowed" reference index.

      StoreIndex<CommitOp> incr = indexesLogic.incrementalIndexFromCommit(commit);
      soft.assertThat(newArrayList(incr))
          .allSatisfy(el -> assertThat(el.content().action()).isIn(ADD, INCREMENTAL_ADD));
    }

    soft.assertAll();

    // Remove all keys added above.
    for (int i = REF_INDEX_LIFECYCLE_TEST_CYCLES_EMBEDDED - 1; i >= 0; i--) {
      head = requireNonNull(commitLogic.doCommit(remove.apply(head, i), emptyList())).id();
      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));
      soft.assertThat(commit.referenceIndex()).isNull();
      soft.assertThat(commit.referenceIndexStripes()).isEmpty();
      soft.assertThat(commit.hasReferenceIndex()).isFalse();
      refIndexVerifyKeyExistence(persist, commit, i - 1, key);

      StoreIndex<CommitOp> incr = indexesLogic.incrementalIndexFromCommit(commit);
      soft.assertThat(incr.elementCount()).isEqualTo(i + 1);
      // "incremental remove" operations do not need to be recorded in the incremental index,
      // because there is no "shadowed" reference index.
      soft.assertThat(newArrayList(incr))
          .allSatisfy(el -> assertThat(el.content().action()).isIn(INCREMENTAL_ADD, REMOVE));
    }

    // The incremental index is empty at this point (validated in the last iteration above)
    soft.assertAll();

    // Adds new keys, one per commits.
    // Loop until it has to spill out to reference index stripes...
    for (num = 0; ; num++) {
      head = requireNonNull(commitLogic.doCommit(add.apply(head, num), emptyList())).id();

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));

      refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
      refIndexVerifyKeyExistence(persist, commit, num, key);

      if (!commit.referenceIndexStripes().isEmpty()) {
        // This number might need to be adjusted if/when the serialized index format changes
        soft.assertThat(num).isEqualTo(REF_INDEX_LIFECYCLE_TEST_CYCLES_EMBEDDED);

        // Will get one stripe, if the reference index stripe size is large enough to hold the
        // "spilled out" incremental _and_ the added key
        soft.assertThat(commit.referenceIndexStripes()).hasSize(1);
        soft.assertAll();
        break;
      }
      soft.assertThat(commit.hasReferenceIndex()).isFalse();
      soft.assertThat(commit.referenceIndex()).isNull();
      soft.assertThat(commit.incrementalIndex().size())
          .isLessThanOrEqualTo(REF_INDEX_LIFECYCLE_TEST_INCR_SIZE);

      // Hard stop to prevent an endless loop (just in case)
      assertThat(num).isLessThan(20);

      soft.assertAll();
    }

    // Adds new keys, one per commits.
    // Loop until the amount of allowed reference index stripes per commit is exceeded and
    // a reference index lookup object is created (StandardObjType.INDEX_SEGMENTS).
    for (num++; ; num++) {
      head = commitWithValues(persist, commitLogic, add, contents, num, head);

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));

      refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
      refIndexVerifyKeyExistence(persist, commit, num, key);

      soft.assertThat(commit.hasReferenceIndex()).isTrue();

      soft.assertThat(commit.incrementalIndex().size())
          .isLessThanOrEqualTo(REF_INDEX_LIFECYCLE_TEST_INCR_SIZE);

      if (commit.referenceIndex() != null) {
        soft.assertThat(commit.referenceIndexStripes()).isEmpty();

        // This number might need to be adjusted if/when the serialized index format changes
        soft.assertThat(num).isEqualTo(REF_INDEX_LIFECYCLE_TEST_CYCLES_EXTERNAL);

        soft.assertAll();
        break;
      }

      soft.assertThat(commit.referenceIndexStripes()).isNotEmpty();

      refIndexValidateStripes(persist, commit.referenceIndexStripes());

      // Hard stop to prevent an endless loop (just in case)
      assertThat(num).isLessThan(30);

      soft.assertAll();
    }

    // Adds a couple new keys, one per commits - just to be on the safe side.
    for (num++; num < maxNum; num++) {
      head = commitWithValues(persist, commitLogic, add, contents, num, head);

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));

      refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
      refIndexVerifyKeyExistence(persist, commit, num, key);

      soft.assertThat(commit.incrementalIndex().size())
          .isLessThanOrEqualTo(REF_INDEX_LIFECYCLE_TEST_SEG_SIZE);

      soft.assertThat(commit.hasReferenceIndex()).isTrue();
      soft.assertThat(commit.referenceIndexStripes()).isEmpty();
      soft.assertThat(commit.referenceIndex()).isNotNull();

      refIndexValidateSegments(persist, commit);

      soft.assertAll();
    }

    // Remove keys now... until we're back at "embedded" reference segments list (no longer need
    // StandardObjType.INDEX_SEGMENTS)...
    for (num--; num >= 0; num--) {
      head = requireNonNull(commitLogic.doCommit(remove.apply(head, num), emptyList())).id();

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));

      refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
      refIndexVerifyKeyExistence(persist, commit, num - 1, key);

      if (commit.referenceIndex() == null) {
        soft.assertAll();
        break;
      }

      soft.assertThat(commit.hasReferenceIndex()).isTrue();
      soft.assertThat(commit.referenceIndexStripes()).isEmpty();

      refIndexValidateSegments(persist, commit);

      soft.assertAll();
    }

    soft.assertThat(num).isEqualTo(REF_INDEX_LIFECYCLE_TEST_CYCLES_EXTERNAL_RM);
    soft.assertAll();

    // Remove keys now... until we're done removing all keys...
    // Can't get back to "incremental only" w/o another "spill out".
    for (num--; num >= 0; num--) {
      head = requireNonNull(commitLogic.doCommit(remove.apply(head, num), emptyList())).id();

      CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));

      refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
      refIndexVerifyKeyExistence(persist, commit, num - 1, key);

      if (commit.referenceIndexStripes().isEmpty()) {
        soft.assertAll();
        break;
      }

      soft.assertThat(commit.hasReferenceIndex()).isTrue();
      soft.assertThat(commit.referenceIndex()).isNull();

      refIndexValidateStripes(persist, commit.referenceIndexStripes());

      soft.assertAll();
    }

    // This last commit has one REMOVE (plus maybe some INCREMENTAL_REMOVE) action in the
    // incremental index, which may shadow some reference index stripes.
    CommitObj commit = requireNonNull(persist.fetchTypedObj(head, COMMIT, CommitObj.class));
    refIndexVerifyIncrIndexWIthRef(commit, indexesLogic);
    refIndexVerifyKeyExistence(persist, commit, -1, key);
    soft.assertThat(commit.referenceIndex()).isNull();
  }

  private ObjId commitWithValues(
      Persist persist,
      CommitLogic commitLogic,
      BiFunction<ObjId, Integer, CreateCommit> add,
      IntFunction<List<Obj>> contents,
      int num,
      ObjId head)
      throws ObjNotFoundException, CommitConflictException {
    List<Obj> values = contents.apply(num);
    ObjId[] valueIds = values.stream().map(Obj::id).toArray(ObjId[]::new);
    soft.assertThatThrownBy(() -> persist.fetchObjs(valueIds))
        .isInstanceOf(ObjNotFoundException.class)
        .asInstanceOf(type(ObjNotFoundException.class))
        .extracting(ObjNotFoundException::objIds)
        .asInstanceOf(list(ObjId.class))
        .hasSize(values.size());
    head = requireNonNull(commitLogic.doCommit(add.apply(head, num), values)).id();
    soft.assertThat(persist.fetchObjs(values.stream().map(Obj::id).toArray(ObjId[]::new)))
        .doesNotContainNull();
    return head;
  }

  private void refIndexVerifyKeyExistence(
      Persist persist, CommitObj commit, int num, IntFunction<StoreKey> key) {
    StoreIndex<CommitOp> fullIndex =
        indexesLogic(persist).buildCompleteIndex(commit, Optional.empty());
    List<StoreKey> existingKeys =
        newArrayList(fullIndex).stream()
            .filter(el -> el.content().action().exists())
            .map(StoreIndexElement::key)
            .collect(toList());
    soft.assertThat(existingKeys)
        .describedAs("num=%d", num)
        .containsExactlyElementsOf(
            num < 0 ? emptyList() : rangeClosed(0, num).mapToObj(key).collect(toList()));
  }

  private void refIndexVerifyIncrIndexWIthRef(CommitObj commit, IndexesLogic indexesLogic) {
    StoreIndex<CommitOp> incr = indexesLogic.incrementalIndexFromCommit(commit);
    soft.assertThat(newArrayList(incr))
        .allSatisfy(
            el ->
                assertThat(el.content().action())
                    .isIn(ADD, INCREMENTAL_ADD, REMOVE, INCREMENTAL_REMOVE));
  }

  private void refIndexValidateSegments(Persist persist, CommitObj commit)
      throws ObjNotFoundException {
    IndexSegmentsObj segments =
        persist.fetchTypedObj(
            requireNonNull(commit.referenceIndex()), INDEX_SEGMENTS, IndexSegmentsObj.class);

    refIndexValidateStripes(persist, segments.stripes());
  }

  private void refIndexValidateStripes(Persist persist, List<IndexStripe> stripes)
      throws ObjNotFoundException {
    StoreKey lastKey = keyFromString("");
    for (IndexStripe stripe : stripes) {
      soft.assertThat(stripe.firstKey()).isGreaterThan(lastKey);
      soft.assertThat(stripe.lastKey()).isGreaterThanOrEqualTo(stripe.firstKey());
      lastKey = stripe.lastKey();

      IndexObj idx = persist.fetchTypedObj(stripe.segment(), INDEX, IndexObj.class);
      soft.assertThat(idx.index().size()).isLessThanOrEqualTo(REF_INDEX_LIFECYCLE_TEST_SEG_SIZE);

      StoreIndex<CommitOp> index = deserializeStoreIndex(idx.index(), COMMIT_OP_SERIALIZER);
      soft.assertThat(index.first()).isEqualTo(stripe.firstKey());
      soft.assertThat(index.last()).isEqualTo(stripe.lastKey());
      soft.assertThat(newArrayList(index))
          .allSatisfy(el -> assertThat(el.content().action()).isSameAs(NONE));
    }
  }

  @Test
  public void commitWithReferenceIndex(
      @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1200")
          @NessieStoreConfig(name = CONFIG_MAX_SERIALIZED_INDEX_SIZE, value = "1200")
          @NessiePersist
          Persist persist)
      throws Exception {
    CommitLogic commitLogic = commitLogic(persist);
    IndexesLogic indexesLogic = indexesLogic(persist);

    @SuppressWarnings("InlineMeInliner")
    String twentyFiveChars = Strings.repeat("-aaaa", 5);

    Map<StoreKey, ObjId> knownKeys = new LinkedHashMap<>();

    Set<ObjId> fullObjIds = new HashSet<>();
    Set<ObjId> segmentIds = new HashSet<>();

    ObjId tip = EMPTY_OBJ_ID;
    boolean hadFull = false;
    List<ObjId> commitIds = new ArrayList<>();
    List<ObjId> keyContentIds = new ArrayList<>();
    for (int numKeys = 0; numKeys < 100; ) {
      CreateCommit.Builder createCommit = stdCommit().parentCommitId(tip);

      for (int numAdd = 0; numAdd < 5; numAdd++, numKeys++) {
        StoreKey key = key(numKeys + twentyFiveChars);
        ObjId value = randomObjId();
        knownKeys.put(key, value);

        keyContentIds.add(value);

        createCommit.addAdds(commitAdd(key, 0, value, null, null));
      }
      soft.assertThat(knownKeys.size()).describedAs("num keys: %d", numKeys).isEqualTo(numKeys);

      tip = requireNonNull(commitLogic.doCommit(createCommit.build(), emptyList())).id();
      commitIds.add(tip);
      soft.assertThat(tip).describedAs("num keys: %d", numKeys).isNotNull();

      CommitObj commit = requireNonNull(commitLogic.fetchCommit(tip));
      soft.assertThat(commit).describedAs("num keys: %d", numKeys).isNotNull();

      StoreIndex<CommitOp> commitIndex = indexesLogic.buildCompleteIndexOrEmpty(commit);
      soft.assertThat(commitIndex.elementCount())
          .describedAs("num keys: %d", numKeys)
          .isEqualTo(knownKeys.size())
          .isEqualTo(numKeys);
      soft.assertThat(commitIndex.asKeyList())
          .describedAs("num keys: %d", numKeys)
          .containsExactlyInAnyOrderElementsOf(knownKeys.keySet());
      int n = numKeys;
      soft.assertThat(knownKeys.keySet())
          .describedAs("num keys: %d", numKeys)
          .allSatisfy(
              k -> assertThat(commitIndex.contains(k)).describedAs("num keys: %d", n).isTrue())
          .allSatisfy(
              k -> assertThat(commitIndex.get(k)).describedAs("num keys: %d", n).isNotNull())
          .allSatisfy(
              k ->
                  assertThat(requireNonNull(commitIndex.get(k)).content().value())
                      .describedAs("num keys: %d", n)
                      .isNotNull()
                      .isEqualTo(knownKeys.get(k)));

      Map<StoreKey, ObjId> inIndex =
          stream(spliteratorUnknownSize(commitIndex.iterator(), 0), false)
              .collect(
                  Collectors.toMap(
                      StoreIndexElement::key, el -> requireNonNull(el.content().value())));
      soft.assertThat(inIndex).describedAs("num keys: %d", numKeys).containsAllEntriesOf(knownKeys);

      ObjId referenceIndexId = commit.referenceIndex();
      if (referenceIndexId != null) {
        Obj full = persist.fetchObj(referenceIndexId);
        if (full instanceof IndexSegmentsObj) {
          IndexSegmentsObj referenceIndexObj = (IndexSegmentsObj) full;
          if (!hadFull) {
            soft.assertThat(referenceIndexObj)
                .describedAs("num keys: %d", numKeys)
                .extracting(Obj::id)
                .matches(fullObjIds::add);
            soft.assertThat(referenceIndexObj)
                .describedAs("num keys: %d", numKeys)
                .extracting(IndexSegmentsObj::stripes, list(IndexStripe.class))
                .extracting(IndexStripe::segment)
                .allMatch(segmentIds::add);
          } else if (fullObjIds.add(referenceIndexId)) {
            boolean anyNew = false;
            boolean anyNotNew = false;
            for (IndexStripe indexStripe : referenceIndexObj.stripes()) {
              if (segmentIds.add(indexStripe.segment())) {
                anyNew = true;
              } else {
                anyNotNew = true;
              }
            }
            soft.assertThat(asList(anyNew, anyNotNew))
                .describedAs("num keys: %d", numKeys)
                .containsExactly(true, true);
          } else {
            soft.assertThat(referenceIndexObj)
                .describedAs("num keys: %d", numKeys)
                .extracting(IndexSegmentsObj::stripes, list(IndexStripe.class))
                .extracting(IndexStripe::segment)
                .allMatch(segmentIds::contains);
          }
        } else {
          soft.fail("Should not generate a single index segment, num keys: %d", numKeys);
        }

        hadFull = true;
      }

      soft.assertAll();
    }

    for (int numKeys = 0; numKeys < 100; ) {
      CreateCommit.Builder createCommit = stdCommit().parentCommitId(tip);

      for (int numAdd = 0; numAdd < 5; numAdd++, numKeys++) {
        ObjId expected = keyContentIds.get(numKeys);
        StoreKey key = key(numKeys + twentyFiveChars);
        ObjId value = randomObjId();
        knownKeys.put(key, value);

        createCommit.addAdds(commitAdd(key, 0, value, expected, null));
      }

      tip = requireNonNull(commitLogic.doCommit(createCommit.build(), emptyList())).id();
      commitIds.add(tip);
      soft.assertThat(tip).describedAs("num keys: %d", numKeys).isNotNull();

      CommitObj commit = commitLogic.fetchCommit(tip);
      soft.assertThat(commit).describedAs("num keys: %d", numKeys).isNotNull();
    }

    // Check "updateCommit" as well
    for (ObjId commitId : commitIds) {
      CommitObj commit = requireNonNull(commitLogic.fetchCommit(commitId));
      soft.assertThat(commit).isNotNull();
      StoreIndex<CommitOp> completeIndex =
          indexesLogic.buildCompleteIndex(commit, Optional.empty());
      StoreIndex<CommitOp> index = newStoreIndex(COMMIT_OP_SERIALIZER);
      completeIndex.forEach(index::add);
      commitLogic.updateCommit(
          commitBuilder()
              .from(commit)
              .incrementalIndex(index.serialize())
              .referenceIndex(null)
              .build());
      CommitObj commitUpdated = requireNonNull(commitLogic.fetchCommit(commitId));
      soft.assertThat(commitUpdated).isNotNull();
      StoreIndex<CommitOp> updatedIndex =
          indexesLogic.buildCompleteIndex(commitUpdated, Optional.empty());
      soft.assertThat(newArrayList(updatedIndex)).isEqualTo(newArrayList(completeIndex));
    }
  }
}
