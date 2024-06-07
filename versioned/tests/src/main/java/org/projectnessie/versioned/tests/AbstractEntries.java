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
package org.projectnessie.versioned.tests;

import static com.google.common.collect.Lists.newArrayList;
import static org.assertj.core.groups.Tuple.tuple;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.KeyEntry;
import org.projectnessie.versioned.Ref;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.VersionStore.KeyRestrictions;
import org.projectnessie.versioned.paging.PaginationIterator;
import org.projectnessie.versioned.testworker.OnRefOnly;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractEntries extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractEntries(VersionStore store) {
    super(store);
  }

  @Test
  public void entriesWrongParameters() {
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                keysAsList(
                    store().noAncestorHash(),
                    KeyRestrictions.builder()
                        .minKey(ContentKey.of("foo"))
                        .maxKey(ContentKey.of("foo"))
                        .prefixKey(ContentKey.of("foo"))
                        .build()))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                keysAsList(
                    store().noAncestorHash(),
                    KeyRestrictions.builder()
                        .maxKey(ContentKey.of("foo"))
                        .prefixKey(ContentKey.of("foo"))
                        .build()))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
    soft.assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                keysAsList(
                    store().noAncestorHash(),
                    KeyRestrictions.builder()
                        .minKey(ContentKey.of("foo"))
                        .prefixKey(ContentKey.of("foo"))
                        .build()))
        .withMessageContaining(
            "Combining prefixKey with either minKey or maxKey is not supported.");
  }

  @Test
  public void entriesRanges() throws Exception {
    BranchName branch = BranchName.of("foo");
    ContentKey key1 = ContentKey.of("k1");
    ContentKey key2 = ContentKey.of("k2");
    ContentKey key2a = ContentKey.of("k2", "a");
    ContentKey key2b = ContentKey.of("k2", "aπ"); // UNICODE CHAR
    ContentKey key2c = ContentKey.of("k2", "πa"); // UNICODE CHAR, This is GREATER than k2.k3 !
    ContentKey key2d = ContentKey.of("k2", "aa");
    ContentKey key23 = ContentKey.of("k2", "k3");
    ContentKey key23a = ContentKey.of("k2", "k3", "a");
    ContentKey key23b = ContentKey.of("k2", "k3", "b");
    ContentKey key3 = ContentKey.of("k3");
    store().create(branch, Optional.empty()).getHash();
    Hash initialCommit =
        commit("Initial Commit")
            .put(key1, newOnRef("v1"))
            .put(key2, Namespace.of(key2))
            .put(key23, Namespace.of(key23))
            .put(key2a, newOnRef("v2a"))
            .put(key2b, newOnRef("v2b"))
            .put(key2c, newOnRef("v2c"))
            .put(key2d, newOnRef("v2d"))
            .put(key23a, newOnRef("v23a"))
            .put(key23b, newOnRef("v23b"))
            .put(key3, newOnRef("v3"))
            .toBranch(branch);

    soft.assertThat(keysAsList(initialCommit, NO_KEY_RESTRICTIONS))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(
            key1, key2, key2a, key2b, key2c, key2d, key23, key23a, key23b, key3);
    soft.assertThat(keysAsList(initialCommit, KeyRestrictions.builder().minKey(key23).build()))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(key23, key23a, key23b, key3, key2c);
    soft.assertThat(keysAsList(initialCommit, KeyRestrictions.builder().prefixKey(key23).build()))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(key23, key23a, key23b);
    soft.assertThat(keysAsList(initialCommit, KeyRestrictions.builder().maxKey(key23).build()))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(key1, key2, key2a, key2b, key2d, key23);
    soft.assertThat(
            keysAsList(
                initialCommit, KeyRestrictions.builder().minKey(key23).maxKey(key23a).build()))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(key23, key23a);
    soft.assertThat(
            keysAsList(
                initialCommit, KeyRestrictions.builder().prefixKey(ContentKey.of("k")).build()))
        .isEmpty();
    soft.assertThat(
            keysAsList(initialCommit, KeyRestrictions.builder().maxKey(ContentKey.of("k")).build()))
        .isEmpty();
    soft.assertThat(
            keysAsList(
                initialCommit, KeyRestrictions.builder().prefixKey(ContentKey.of("x")).build()))
        .isEmpty();
    soft.assertThat(
            keysAsList(
                initialCommit,
                KeyRestrictions.builder()
                    .contentKeyPredicate((k, t) -> k.toPathString().startsWith(key2.toPathString()))
                    .build()))
        .map(e -> e.getKey().contentKey())
        .containsExactlyInAnyOrder(key2, key2a, key2b, key2c, key2d, key23, key23a, key23b);
  }

  List<KeyEntry> keysAsList(Ref ref, KeyRestrictions keyRestrictions) throws Exception {
    try (PaginationIterator<KeyEntry> keys = store().getKeys(ref, null, false, keyRestrictions)) {
      return newArrayList(keys);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 49, 50, 51, 100, 101})
  void getKeys(int numKeys) throws Exception {
    BranchName branch = BranchName.of("foo");
    Hash head = store().create(branch, Optional.empty()).getHash();

    List<Tuple> expected = new ArrayList<>();
    if (numKeys > 0) {
      CommitBuilder commit = commit("Initial Commit");
      for (int i = 0; i < numKeys; i++) {
        ContentKey key = ContentKey.of("key" + i);
        OnRefOnly value = newOnRef("key" + i);
        commit.put(key, value);
        expected.add(tuple(key, value));
      }
      head = commit.toBranch(branch);
    }

    try (PaginationIterator<KeyEntry> iter =
        store().getKeys(head, null, true, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(iter)
          .toIterable()
          .extracting(
              keyEntry -> keyEntry.getKey().contentKey(),
              keyEntry -> keyEntry.getContent().withId(null))
          .containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  @Test
  void entries() throws Exception {
    BranchName branch = BranchName.of("foo");
    ContentKey key2 = ContentKey.of("k2");
    ContentKey key2a = ContentKey.of("k2", "a");
    ContentKey key23 = ContentKey.of("k2", "k3");
    ContentKey key23a = ContentKey.of("k2", "k3", "a");
    store().create(branch, Optional.empty()).getHash();
    Hash commit =
        commit("Initial Commit")
            .put(key2, Namespace.of(key2))
            .put(key23, Namespace.of(key23))
            .put(key2a, newOnRef("v2a"))
            .put(key23a, newOnRef("v23a"))
            .toBranch(branch);

    ContentResult content2 = store().getValue(commit, key2, false);
    ContentResult content2a = store().getValue(commit, key2a, false);
    ContentResult content23 = store().getValue(commit, key23, false);
    ContentResult content23a = store().getValue(commit, key23a, false);

    try (PaginationIterator<KeyEntry> iter =
        store.getKeys(commit, null, true, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(iter)
          .toIterable()
          .extracting(KeyEntry::getKey, KeyEntry::getContent)
          .containsExactlyInAnyOrder(
              tuple(content2.identifiedKey(), content2.content()),
              tuple(content2a.identifiedKey(), content2a.content()),
              tuple(content23.identifiedKey(), content23.content()),
              tuple(content23a.identifiedKey(), content23a.content()));
    }

    soft.assertThat(store.getIdentifiedKeys(commit, newArrayList(key2, key2a, key23, key23a)))
        .containsExactly(
            content2.identifiedKey(),
            content2a.identifiedKey(),
            content23.identifiedKey(),
            content23a.identifiedKey());

    soft.assertThat(content2a.identifiedKey().elements())
        .startsWith(
            content2
                .identifiedKey()
                .elements()
                .toArray(new IdentifiedContentKey.IdentifiedElement[0]));
    soft.assertThat(content23.identifiedKey().elements())
        .startsWith(
            content2
                .identifiedKey()
                .elements()
                .toArray(new IdentifiedContentKey.IdentifiedElement[0]));
    soft.assertThat(content23a.identifiedKey().elements())
        .startsWith(
            content23
                .identifiedKey()
                .elements()
                .toArray(new IdentifiedContentKey.IdentifiedElement[0]));

    commit = commit("Second Commit").delete(key2a).toBranch(branch);

    try (PaginationIterator<KeyEntry> iter =
        store.getKeys(commit, null, true, NO_KEY_RESTRICTIONS)) {
      soft.assertThat(iter)
          .toIterable()
          .extracting(KeyEntry::getKey, KeyEntry::getContent)
          .containsExactlyInAnyOrder(
              tuple(content2.identifiedKey(), content2.content()),
              tuple(content23.identifiedKey(), content23.content()),
              tuple(content23a.identifiedKey(), content23a.content()));
    }

    soft.assertThat(store.getIdentifiedKeys(commit, newArrayList(key2a)))
        .containsOnly(content2a.identifiedKey());
  }
}
