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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.projectnessie.model.IdentifiedContentKey.identifiedContentKeyFromContent;
import static org.projectnessie.versioned.ContentResult.contentResult;
import static org.projectnessie.versioned.testworker.OnRefOnly.newOnRef;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IdentifiedContentKey;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.VersionStore;

@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractContents extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractContents(VersionStore store) {
    super(store);
  }

  @Test
  public void getValueNonExisting() throws Exception {
    BranchName branch = BranchName.of("empty-branch");
    store().create(branch, Optional.empty());
    Hash hash = store().hashOnReference(branch, Optional.empty(), emptyList());

    ContentKey key1 = ContentKey.of("key1");
    ContentKey keyNs = ContentKey.of("ns");
    ContentKey key2 = ContentKey.of("ns", "key2");
    IdentifiedContentKey notFoundIdentifiedKey1 =
        identifiedContentKeyFromContent(key1, null, null, l -> null);
    IdentifiedContentKey notFoundIdentifiedKey2 =
        identifiedContentKeyFromContent(key2, null, null, l -> null);

    // Verify non-existing keys against a non-existing commit ("beginning of epoch")

    soft.assertThat(store().getValue(hash, key1, false)).isNull();
    soft.assertThat(store().getValues(hash, List.of(key1), false)).isEmpty();
    soft.assertThat(store().getValue(hash, key1, true))
        .isEqualTo(contentResult(notFoundIdentifiedKey1, null, null));

    soft.assertThat(store().getValues(hash, List.of(key1, key2), false)).isEmpty();
    soft.assertThat(store().getValues(hash, List.of(key1, key2), true))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                key1, contentResult(notFoundIdentifiedKey1, null, null),
                key2, contentResult(notFoundIdentifiedKey2, null, null)));

    hash = commit("Initial Commit").put(keyNs, Namespace.of(keyNs)).toBranch(branch);

    String contentIdNs = requireNonNull(store().getValue(hash, keyNs, false).content()).getId();
    notFoundIdentifiedKey2 =
        identifiedContentKeyFromContent(
            key2, null, null, l -> List.of("ns").equals(l) ? contentIdNs : null);

    // Verify non-existing keys against an existing commit

    soft.assertThat(store().getValue(hash, key1, false)).isNull();
    soft.assertThat(store().getValues(hash, List.of(key1), false)).isEmpty();
    soft.assertThat(store().getValue(hash, key1, true))
        .isEqualTo(contentResult(notFoundIdentifiedKey1, null, null));

    soft.assertThat(store().getValues(hash, List.of(key1, key2), false)).isEmpty();
    soft.assertThat(store().getValues(hash, List.of(key1, key2), true))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                key1, contentResult(notFoundIdentifiedKey1, null, null),
                key2, contentResult(notFoundIdentifiedKey2, null, null)));
  }

  @Test
  void recreateTable() throws Exception {
    BranchName branch = BranchName.of("recreateTable-main");
    ContentKey key = ContentKey.of("recreateTable");

    store().create(branch, Optional.empty());
    // commit just something to have a "real" common ancestor and not "beginning of time", which
    // means no-common-ancestor
    Content initialState = newOnRef("value");
    CommitResult ancestor =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("create table"),
                singletonList(Put.of(key, initialState)));
    soft.assertThat(contentWithoutId(store().getValue(branch, key, false))).isEqualTo(initialState);
    soft.assertThat(contentWithoutId(store().getValue(ancestor.getCommitHash(), key, false)))
        .isEqualTo(initialState);

    CommitResult delete =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("drop table"),
                ImmutableList.of(Delete.of(key)));
    soft.assertThat(store().getValue(branch, key, false)).isNull();
    soft.assertThat(store().getValue(delete.getCommitHash(), key, false)).isNull();

    Content recreateState = newOnRef("value");
    CommitResult recreate =
        store()
            .commit(
                branch,
                Optional.empty(),
                CommitMeta.fromMessage("drop table"),
                ImmutableList.of(Put.of(key, recreateState)));
    soft.assertThat(contentWithoutId(store().getValue(branch, key, false)))
        .isEqualTo(recreateState);
    soft.assertThat(contentWithoutId(store().getValue(recreate.getCommitHash(), key, false)))
        .isEqualTo(recreateState);
  }
}
