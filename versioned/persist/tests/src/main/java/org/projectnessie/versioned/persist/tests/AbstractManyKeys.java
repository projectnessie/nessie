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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.Mockito.spy;
import static org.projectnessie.versioned.store.DefaultStoreWorker.payloadForContent;
import static org.projectnessie.versioned.testworker.OnRefOnly.onRef;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.spi.AbstractDatabaseAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapter;
import org.projectnessie.versioned.persist.tests.extension.NessieDbAdapterConfigItem;
import org.projectnessie.versioned.store.DefaultStoreWorker;
import org.projectnessie.versioned.testworker.OnRefOnly;

/**
 * Verifies that a big-ish number of keys, split across multiple commits works and the correct
 * results are returned for the commit-log, keys, global-states. This test is especially useful to
 * verify that the embedded and nested key-lists (think: full-key-lists in a commit-log-entry) work
 * correctly.
 */
public abstract class AbstractManyKeys {

  private final DatabaseAdapter databaseAdapter;

  protected AbstractManyKeys(DatabaseAdapter databaseAdapter) {
    this.databaseAdapter = databaseAdapter;
  }

  static class ManyKeysParams {
    final int keys;
    final int commits;

    public ManyKeysParams(int keys, int commits) {
      this.keys = keys;
      this.commits = commits;
    }

    @Override
    public String toString() {
      return "keys=" + keys + ", commits=" + commits;
    }
  }

  static List<ManyKeysParams> manyKeysParams() {
    return Arrays.asList(
        new ManyKeysParams(1000, 25),
        new ManyKeysParams(100, 10),
        new ManyKeysParams(500, 2),
        new ManyKeysParams(500, 10));
  }

  @ParameterizedTest
  @MethodSource("manyKeysParams")
  void manyKeys(ManyKeysParams params) throws Exception {
    BranchName main = BranchName.of("main");

    List<ImmutableCommitParams.Builder> commits =
        IntStream.range(0, params.commits)
            .mapToObj(
                i ->
                    ImmutableCommitParams.builder()
                        .commitMetaSerialized(ByteString.copyFromUtf8("commit #" + i))
                        .toBranch(main))
            .collect(Collectors.toList());
    AtomicInteger commitDist = new AtomicInteger();

    Set<ContentKey> allKeys = new HashSet<>();

    IntStream.range(0, params.keys)
        .mapToObj(
            i -> {
              ContentKey key =
                  ContentKey.of(
                      "some-"
                          + i
                          + "-long-key-value-foobarbazfoobarbazfoobarbazfoobarbazfoobarbazfoobarbaz");
              allKeys.add(key);
              OnRefOnly val = onRef("value " + i, "cid-" + i);
              return KeyWithBytes.of(
                  key, ContentId.of(val.getId()), (byte) payloadForContent(val), val.serialized());
            })
        .forEach(kb -> commits.get(commitDist.incrementAndGet() % params.commits).addPuts(kb));

    for (ImmutableCommitParams.Builder commit : commits) {
      databaseAdapter.commit(commit.build());
    }

    Hash mainHead = databaseAdapter.hashOnReference(main, Optional.empty());
    try (Stream<KeyListEntry> keys = databaseAdapter.keys(mainHead, KeyFilterPredicate.ALLOW_ALL)) {
      List<ContentKey> fetchedKeys = keys.map(KeyListEntry::getKey).collect(Collectors.toList());

      // containsExactlyInAnyOrderElementsOf() is quite expensive and slow with ContentKey's
      // implementation of 'ContentKey.equals()' since it uses a collator.
      List<String> fetchedKeysStrings =
          fetchedKeys.stream().map(ContentKey::toString).collect(Collectors.toList());
      List<String> allKeysStrings =
          allKeys.stream().map(ContentKey::toString).collect(Collectors.toList());

      assertThat(fetchedKeysStrings)
          .hasSize(allKeysStrings.size())
          .containsExactlyInAnyOrderElementsOf(allKeysStrings);
    }
  }

  /**
   * Nessie versions before 0.22.0 write key-lists without the ID of the commit that last updated
   * the key. This test ensures that the commit-ID is added for newly written key-lists.
   */
  @Test
  void enhanceKeyListWithCommitId(
      @NessieDbAdapter
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "5")
          @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "100")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "100")
          DatabaseAdapter da)
      throws Exception {

    // All other database adapter implementation don't work, because they use "INSERT"-ish (no
    // "upsert"), so the code to "break" the key-lists below, which is essential for the test to
    // work, will fail.
    assumeThat(da)
        .extracting(Object::getClass)
        .extracting(Class::getSimpleName)
        .isIn("InmemoryDatabaseAdapter", "RocksDatabaseAdapter");

    // Because we cannot write "old" KeyListEntry's, write a series of keys + commits here and then
    // "break" the last written key-list-entities by removing the commitID. Then write a new
    // key-list
    // and verify that the key-list-entries have the (right) commit IDs.

    @SuppressWarnings("unchecked")
    AbstractDatabaseAdapter<AutoCloseable, ?> ada = (AbstractDatabaseAdapter<AutoCloseable, ?>) da;

    int keyListDistance = ada.getConfig().getKeyListDistance();

    String longString =
        "keykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykey"
            + "keykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykeykey";

    BranchName branch = BranchName.of("main");
    ByteString meta = ByteString.copyFromUtf8("msg");

    Map<ContentKey, Hash> keyToCommit = new HashMap<>();
    for (int i = 0; i < 10 * keyListDistance; i++) {
      ContentKey key = ContentKey.of("k" + i + "-" + longString);
      Hash hash =
          da.commit(
                  ImmutableCommitParams.builder()
                      .toBranch(branch)
                      .commitMetaSerialized(meta)
                      .addPuts(
                          KeyWithBytes.of(
                              key,
                              ContentId.of("c" + i),
                              (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
                              DefaultStoreWorker.instance()
                                  .toStoreOnReferenceState(onRef("r" + i, "c" + i))))
                      .build())
              .getCommit()
              .getHash();
      keyToCommit.put(key, hash);
    }

    Hash head = da.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    try (AutoCloseable ctx = ada.borrowConnection()) {
      // Sanity: fetchKeyLists called exactly once
      AbstractDatabaseAdapter<AutoCloseable, ?> adaSpy = spy(ada);
      adaSpy.values(head, keyToCommit.keySet(), KeyFilterPredicate.ALLOW_ALL);

      CommitLogEntry commit = ada.fetchFromCommitLog(ctx, head);
      assertThat(commit).isNotNull();
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {

        List<KeyListEntity> noCommitIds =
            keyListEntityStream
                .map(
                    e ->
                        KeyListEntity.of(
                            e.getId(),
                            KeyList.of(
                                e.getKeys().getKeys().stream()
                                    .filter(Objects::nonNull)
                                    .map(
                                        k ->
                                            KeyListEntry.of(
                                                k.getKey(), k.getContentId(), k.getPayload(), null))
                                    .collect(Collectors.toList()))))
                .collect(Collectors.toList());
        ada.writeKeyListEntities(ctx, noCommitIds);
      }

      // Cross-check that commitIDs in all KeyListEntry is null.
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {
        assertThat(keyListEntityStream)
            .allSatisfy(
                kl ->
                    assertThat(kl.getKeys().getKeys())
                        .allSatisfy(e -> assertThat(e.getCommitId()).isNull()));
      }
    }

    for (int i = 0; i < keyListDistance; i++) {
      ContentKey key = ContentKey.of("pre-fix-" + i);
      Hash hash =
          da.commit(
                  ImmutableCommitParams.builder()
                      .toBranch(branch)
                      .commitMetaSerialized(meta)
                      .addPuts(
                          KeyWithBytes.of(
                              key,
                              ContentId.of("c" + i),
                              (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
                              DefaultStoreWorker.instance()
                                  .toStoreOnReferenceState(onRef("pf" + i, "cpf" + i))))
                      .build())
              .getCommit()
              .getHash();
      keyToCommit.put(key, hash);
    }

    Hash fixedHead = da.namedRef(branch.getName(), GetNamedRefsParams.DEFAULT).getHash();

    try (AutoCloseable ctx = ada.borrowConnection()) {
      CommitLogEntry commit = ada.fetchFromCommitLog(ctx, fixedHead);
      assertThat(commit).isNotNull();

      // Check that commitIDs in all KeyListEntry is NOT null and points to the expected commitId.
      try (Stream<KeyListEntity> keyListEntityStream =
          ada.fetchKeyLists(ctx, commit.getKeyListsIds())) {
        assertThat(keyListEntityStream)
            .allSatisfy(
                kl ->
                    assertThat(kl.getKeys().getKeys())
                        .filteredOn(Objects::nonNull)
                        .allSatisfy(
                            e ->
                                assertThat(e)
                                    .extracting(KeyListEntry::getCommitId, KeyListEntry::getKey)
                                    .containsExactly(keyToCommit.get(e.getKey()), e.getKey())));
      }
    }
  }

  @Test
  void pointContentKeyLookups(
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "16384")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "16384")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws Exception {

    // generate a commits that lead to (at least) 5 key-list entities

    // 64 chars
    String keyElement = "1234567890123456789012345678901234567890123456789012345678901234";
    // generates "long" keys
    IntFunction<ContentKey> keyGen =
        i -> ContentKey.of("k-" + i + "-" + keyElement + "-" + keyElement);
    IntFunction<OnRefOnly> valueGen = i -> onRef("value-" + i, "cid-" + i);

    BranchName branch = BranchName.of("main");
    Hash head = databaseAdapter.hashOnReference(branch, Optional.empty());

    // It's bigger in reality...
    int assumedKeyEntryLen = 160;
    int estimatedTotalKeyLength = 5 * 16384 + 16384;
    int keyNum;
    for (keyNum = 0;
        estimatedTotalKeyLength > 0;
        keyNum++, estimatedTotalKeyLength -= assumedKeyEntryLen) {
      OnRefOnly val = valueGen.apply(keyNum);
      head =
          databaseAdapter
              .commit(
                  ImmutableCommitParams.builder()
                      .toBranch(branch)
                      .commitMetaSerialized(ByteString.EMPTY)
                      .addPuts(
                          KeyWithBytes.of(
                              keyGen.apply(keyNum),
                              ContentId.of(val.getId()),
                              (byte) payloadForContent(val),
                              val.serialized()))
                      .build())
              .getCommit()
              .getHash();
    }

    for (int i = 0; i < keyNum; i++) {
      ContentKey key = keyGen.apply(i);
      Map<ContentKey, ContentAndState> values =
          databaseAdapter.values(
              head, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);
      assertThat(values)
          .extractingByKey(key)
          .extracting(ContentAndState::getRefState)
          .isEqualTo(valueGen.apply(i).serialized());
    }
  }

  /**
   * Exercise a key list with segments containing one entry each.
   *
   * <p>For background, I found that key list goal sizes too small to fit a single entry caused
   * lookup failures on buckets within the highest-indexed segment.
   *
   * <p>When the config parameter max.key.list.size is small or zero, the effective max embedded key
   * list size computed in {@link AbstractDatabaseAdapter#buildKeyList(AutoCloseable,
   * CommitLogEntry, Consumer, Function)} can go negative. This makes {@link
   * org.projectnessie.versioned.persist.adapter.spi.KeyListBuildState} produce a size-zero embedded
   * key list. If the external key lists also have size 1, then the number of segments exceeds the
   * number of buckets, which contributes to {@link
   * org.projectnessie.versioned.persist.adapter.spi.FetchValuesUsingOpenAddressing#segmentForKey(int,
   * int)} computing the wrong segment index for the final key, since it clamps a segment index to
   * the bucket index interval. Applying the open-addressing bucket mask to the segment index is
   * normally a no-op, except in the extreme edge case described above.
   *
   * <p>It was possible to induce this bug without setting the goal sizes all the way down to zero.
   * It just required values small enough to generate a size-zero embedded key list and a series of
   * size-one external key lists after that.
   *
   * <p>This case is covered with greater specificity in the {@code segmentWrapAround*} methods in
   * {@linkplain
   * org.projectnessie.versioned.persist.adapter.spi.TestFetchValuesUsingOpenAddressing}.
   */
  @Test
  void pathologicallySmallSegments(
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "0")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "0")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapterConfigItem(name = "key.list.hash.load.factor", value = "1.0")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    IntFunction<ContentKey> keyGen = i -> ContentKey.of("k-" + i);
    IntFunction<OnRefOnly> valueGen = i -> onRef("value-" + i, "cid-" + i);
    BranchName branch = BranchName.of("main");

    // This should be a power of two, so that the entries hashed by KeyListBuildState completely
    // fill its buckets, but this specific power is an arbitrary choice and not significant.
    final int keyCount = 4;

    commitPutsOnGeneratedKeys(databaseAdapter, branch, keyGen, valueGen, keyCount);

    Hash head = makeEmptyCommits(databaseAdapter, branch, 20);

    checkKeysAndValuesIndividually(databaseAdapter, head, keyGen, valueGen, keyCount);
  }

  /**
   * Test a completely full hashmap where each segment contains two elements.
   *
   * <p>This is the integration-test counterpart of the unit-test {@code
   * TestFetchValuesUsingOpenAddressing#segmentWrapAroundWithPresentKey}.
   *
   * <p>When this failed, examining with a debugger showed an attempt to read a key from the final
   * segment. The read missed in the final segment, though it was present in another segment
   * (presumably due to all the collisions caused by the undersized table). Code surrounding {@link
   * org.projectnessie.versioned.persist.adapter.spi.FetchValuesUsingOpenAddressing} would increment
   * the round-count from zero to one, then retry. This generated a segment index that ran off the
   * end of the segment space, which {@link
   * org.projectnessie.versioned.persist.adapter.spi.FetchValuesUsingOpenAddressing#checkForKeys(int,
   * Collection, Consumer)} would ignore, treating it as a miss, and not including it in the set of
   * keys to be tried on the next round.
   *
   * <p>The figures {@code max.key.list(.entity).size} are hand-picked to leave 130 bytes in both
   * the embedded segment (after embedded-segment-specific overhead) and the external segments of
   * the key list. This is brittle, and could be more robustly derived from {@link
   * AbstractDatabaseAdapter#entitySize(KeyListEntry)}, but I didn't want to widen the visibility
   * modifier on that just for testing, besides having to configure the store in a way inconsistent
   * with the rest of this test. It's also brittle in the sense that the adapter implementations
   * currently serialize and size {@code KeyListEntry} instances in the same way, but that could
   * theoretically change in the future, in which case there would be no one-size-fits-all set of
   * annotation values. I was willing to tolerate these drawbacks because, at worst, changes in the
   * underlying {@code KeyListEntry} serialization that invalidate this test's assumptions could
   * cause false passage, but could not cause false failure.
   */
  @Test
  void pathologicallyFrequentCollisions(
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "892")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "130")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapterConfigItem(name = "key.list.hash.load.factor", value = "1.0")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws ReferenceNotFoundException, ReferenceConflictException {

    IntFunction<ContentKey> keyGen = i -> ContentKey.of("k-" + i);
    IntFunction<OnRefOnly> valueGen = i -> onRef("value-" + i, "cid-" + i);
    BranchName branch = BranchName.of("main");
    // This should be a power of two, so that the entries hashed by KeyListBuildState completely
    // fill its buckets, but this specific power is an arbitrary choice and not significant.
    final int keyCount = 128;

    // Prepare keyCount unique keys, then put them in a single commit
    commitPutsOnGeneratedKeys(databaseAdapter, branch, keyGen, valueGen, keyCount);

    // Repeatedly commit until we exceed the key.list.distance, triggering a key summary
    Hash head = makeEmptyCommits(databaseAdapter, branch, 20);

    checkKeysAndValuesIndividually(databaseAdapter, head, keyGen, valueGen, keyCount);
  }

  /** Commit once, with puts from applying supplied functions to the ints {@code [0, keyCount)}. */
  private static void commitPutsOnGeneratedKeys(
      DatabaseAdapter databaseAdapter,
      BranchName branch,
      IntFunction<ContentKey> keyGen,
      IntFunction<OnRefOnly> valueGen,
      int keyCount)
      throws ReferenceNotFoundException, ReferenceConflictException {
    List<KeyWithBytes> keys =
        IntStream.range(0, keyCount)
            .mapToObj(
                i -> {
                  OnRefOnly val = valueGen.apply(i);
                  return KeyWithBytes.of(
                      keyGen.apply(i),
                      ContentId.of(val.getId()),
                      (byte) payloadForContent(val),
                      val.serialized());
                })
            .collect(Collectors.toCollection(() -> new ArrayList<>(keyCount)));
    databaseAdapter.commit(
        ImmutableCommitParams.builder()
            .toBranch(branch)
            .commitMetaSerialized(ByteString.EMPTY)
            .addAllPuts(keys)
            .build());
  }

  /** Make an empty commit to the supplied branch, {@code commitCount} times. */
  private static Hash makeEmptyCommits(
      DatabaseAdapter databaseAdapter, BranchName toBranch, int commitCount)
      throws ReferenceNotFoundException, ReferenceConflictException {
    Preconditions.checkArgument(0 < commitCount);
    Hash head = null;
    for (int i = 0; i < commitCount; i++) {
      head =
          databaseAdapter
              .commit(
                  ImmutableCommitParams.builder()
                      .toBranch(toBranch)
                      .commitMetaSerialized(ByteString.EMPTY)
                      .build())
              .getCommit()
              .getHash();
    }
    Preconditions.checkNotNull(head);
    return head;
  }

  /** Assert that every key and its expected value can be read from the supplied head. */
  private static void checkKeysAndValuesIndividually(
      DatabaseAdapter databaseAdapter,
      Hash head,
      IntFunction<ContentKey> keyGen,
      IntFunction<OnRefOnly> valueGen,
      int keyCount)
      throws ReferenceNotFoundException {
    for (int i = 0; i < keyCount; i++) {
      ContentKey key = keyGen.apply(i);
      Map<ContentKey, ContentAndState> values =
          databaseAdapter.values(
              head, Collections.singletonList(key), KeyFilterPredicate.ALLOW_ALL);
      assertThat(values)
          .extractingByKey(key)
          .extracting(ContentAndState::getRefState)
          .isEqualTo(valueGen.apply(i).serialized());
    }
  }

  public static Stream<List<String>> progressivelyManyKeyNames() {
    // The seed of `7` was chosen after a few manual tries because it caused the most test failures
    // in the key lookup code before PR #5145, where this test was introduced.
    Random random = new Random(7);
    return IntStream.range(1, 100)
        // Use two test args of the same size (with random, deterministic values)
        .flatMap(i -> IntStream.of(i, i))
        .mapToObj(
            i ->
                IntStream.range(1, i)
                    .mapToObj(
                        j -> Long.toString(random.nextLong() & Long.MAX_VALUE, Character.MAX_RADIX))
                    .collect(Collectors.toList()));
  }

  /**
   * Validates key construction and lookup with random, yet deterministic key names hoping to cover
   * all edge cases and hash collisions by using a somewhat large test data set.
   *
   * <p>Note: the number of keys that is effective for validating the hash lookup indirectly depends
   * on the "embedded" and "external" key list entity size limits, with which the adapter is
   * configured.
   */
  @ParameterizedTest
  @MethodSource("progressivelyManyKeyNames")
  @DisabledIfSystemProperty(
      named = "nessie.integrationTest",
      matches = "true",
      disabledReason = "runs too long for integration tests, non-IT coverage's sufficient")
  void manyKeysProgressive(
      List<String> names,
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "2048")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "1000000")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapterConfigItem(name = "key.list.hash.load.factor", value = "0.65")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws Exception {
    testManyKeysProgressive(names, databaseAdapter);
  }

  /**
   * Same as {@link #manyKeysProgressive(List, DatabaseAdapter)} but uses zero key list size limits,
   * which caused the "embedded" key list segment to be empty, and the external key list "entities"
   * to have at most one entry each.
   */
  @ParameterizedTest
  @MethodSource("progressivelyManyKeyNames")
  @DisabledIfSystemProperty(
      named = "nessie.integrationTest",
      matches = "true",
      disabledReason = "runs too long for integration tests, non-IT coverage's sufficient")
  void manyKeysProgressiveSmallLists(
      List<String> names,
      @NessieDbAdapterConfigItem(name = "max.key.list.size", value = "0")
          @NessieDbAdapterConfigItem(name = "max.key.list.entity.size", value = "0")
          @NessieDbAdapterConfigItem(name = "key.list.distance", value = "20")
          @NessieDbAdapterConfigItem(name = "key.list.hash.load.factor", value = "0.7")
          @NessieDbAdapter
          DatabaseAdapter databaseAdapter)
      throws Exception {
    testManyKeysProgressive(names, databaseAdapter);
  }

  private void testManyKeysProgressive(List<String> names, DatabaseAdapter databaseAdapter)
      throws Exception {
    BranchName main = BranchName.of("main");
    Hash head = databaseAdapter.hashOnReference(main, Optional.empty());
    Set<ContentKey> activeKeys = new HashSet<>();
    for (String name : names) {
      ContentKey key = ContentKey.of(name);
      head =
          databaseAdapter
              .commit(
                  ImmutableCommitParams.builder()
                      .toBranch(main)
                      .commitMetaSerialized(ByteString.copyFromUtf8("foo"))
                      .expectedHead(Optional.of(head))
                      .addPuts(
                          KeyWithBytes.of(
                              key,
                              ContentId.of("id-" + name),
                              (byte) payloadForContent(OnRefOnly.ON_REF_ONLY),
                              DefaultStoreWorker.instance()
                                  .toStoreOnReferenceState(OnRefOnly.newOnRef("c" + name))))
                      .build())
              .getCommit()
              .getHash();
      activeKeys.add(key);
    }

    Map<ContentKey, ContentAndState> values =
        databaseAdapter.values(head, activeKeys, KeyFilterPredicate.ALLOW_ALL);
    assertThat(values.keySet()).containsExactlyInAnyOrderElementsOf(activeKeys);
  }
}
