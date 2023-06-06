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
package org.projectnessie.services.impl;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.model.CommitMeta.fromMessage;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Detached;
import org.projectnessie.model.DiffResponse.DiffEntry;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.model.Reference;

public abstract class AbstractTestDiff extends BaseTestServiceImpl {

  public static Stream<Object[]> diffRefModes() {
    return Arrays.stream(ReferenceMode.values())
        .flatMap(
            refModeFrom ->
                Arrays.stream(ReferenceMode.values())
                    .map(refModeTo -> new Object[] {refModeFrom, refModeTo}));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 20, 22})
  public void diffPaging(int numKeys) throws BaseNessieClientServerException {
    Branch defaultBranch = treeApi().getDefaultBranch();
    Branch branch =
        ensureNamespacesForKeysExist(createBranch("entriesPaging"), ContentKey.of("key", "0"));

    IntFunction<ContentKey> contentKey = i -> ContentKey.of("key", Integer.toString(i));
    IntFunction<IcebergTable> table = i -> IcebergTable.of("meta" + i, 1, 2, 3, 4);
    int pageSize = 5;

    if (numKeys > 0) {
      branch =
          commit(
                  branch,
                  fromMessage("commit"),
                  IntStream.range(0, numKeys)
                      .mapToObj(i -> Put.of(contentKey.apply(i), table.apply(i)))
                      .toArray(Operation[]::new))
              .getTargetBranch();
    }

    AtomicReference<Reference> effectiveFrom = new AtomicReference<>();
    AtomicReference<Reference> effectiveTo = new AtomicReference<>();

    Set<ContentKey> contents =
        pagedDiff(branch, defaultBranch, pageSize, numKeys, effectiveFrom::set, effectiveTo::set)
            .stream()
            .filter(
                e ->
                    (e.getTo() == null ? e.getFrom() : e.getTo()).getType()
                        != Content.Type.NAMESPACE)
            .map(DiffEntry::getKey)
            .collect(toSet());

    soft.assertThat(effectiveFrom).hasValue(branch);
    soft.assertThat(effectiveTo).hasValue(defaultBranch);

    soft.assertThat(contents)
        .containsExactlyInAnyOrderElementsOf(
            IntStream.range(0, numKeys).mapToObj(contentKey).collect(toSet()));
  }

  @ParameterizedTest
  @MethodSource("diffRefModes")
  public void testDiff(ReferenceMode refModeFrom, ReferenceMode refModeTo)
      throws BaseNessieClientServerException {
    int commitsPerBranch = 3;

    Reference fromRef = createBranch("testDiffFromRef", treeApi().getDefaultBranch());
    Reference toRef = createBranch("testDiffToRef", treeApi().getDefaultBranch());
    String toRefHash = createCommits(toRef, 1, commitsPerBranch, toRef.getHash());

    toRef = Branch.of(toRef.getName(), toRefHash);

    List<DiffEntry> diffOnRefHeadResponse =
        diff(refModeFrom.transform(fromRef), refModeTo.transform(toRef));

    // we only committed to toRef, the "from" diff should be null
    soft.assertThat(diffOnRefHeadResponse)
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // Some combinations with explicit fromHashOnRef/toHashOnRef
    soft.assertThat(diff(fromRef, toRef)).isEqualTo(diffOnRefHeadResponse);

    // Comparing the from-reference with the to-reference @ from-reference-HEAD must yield an empty
    // result
    if (refModeTo != ReferenceMode.NAME_ONLY) {
      Branch toRefAtFrom = Branch.of(toRef.getName(), fromRef.getHash());
      soft.assertThat(diff(refModeFrom.transform(fromRef), refModeTo.transform(toRefAtFrom)))
          .isEmpty();
    }

    // after committing to fromRef, "from/to" diffs should both have data
    fromRef =
        Branch.of(
            fromRef.getName(), createCommits(fromRef, 1, commitsPerBranch, fromRef.getHash()));

    soft.assertThat(diff(refModeFrom.transform(fromRef), refModeTo.transform(toRef)))
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNotNull();

              // we only have a diff on the ID
              assertThat(diff.getFrom().getId()).isNotEqualTo(diff.getTo().getId());
              Optional<IcebergTable> fromTable = diff.getFrom().unwrap(IcebergTable.class);
              assertThat(fromTable).isPresent();
              Optional<IcebergTable> toTable = diff.getTo().unwrap(IcebergTable.class);
              assertThat(toTable).isPresent();

              assertThat(fromTable.get().getMetadataLocation())
                  .isEqualTo(toTable.get().getMetadataLocation());
              assertThat(fromTable.get().getSchemaId()).isEqualTo(toTable.get().getSchemaId());
              assertThat(fromTable.get().getSnapshotId()).isEqualTo(toTable.get().getSnapshotId());
              assertThat(fromTable.get().getSortOrderId())
                  .isEqualTo(toTable.get().getSortOrderId());
              assertThat(fromTable.get().getSpecId()).isEqualTo(toTable.get().getSpecId());
            });

    // request all keys and delete the tables for them on toRef
    Map<ContentKey, Content> map =
        contents(
            toRef,
            IntStream.rangeClosed(0, commitsPerBranch)
                .mapToObj(i -> ContentKey.of("table" + i))
                .toArray(ContentKey[]::new));
    for (Map.Entry<ContentKey, Content> entry : map.entrySet()) {
      toRef =
          commit(toRef.getName(), toRefHash, fromMessage("delete"), Delete.of(entry.getKey()))
              .getTargetBranch();
    }

    // now that we deleted all tables on toRef, the diff for "to" should be null
    soft.assertThat(diff(refModeFrom.transform(fromRef), refModeTo.transform(toRef)))
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNull();
            });
  }

  @Test
  public void testDiffByNamelessReference() throws BaseNessieClientServerException {
    Reference fromRef = createBranch("diffFrom", treeApi().getDefaultBranch());
    Reference toRef = createBranch("diffTo", treeApi().getDefaultBranch());
    String toRefHash = createCommits(toRef, 1, 1, toRef.getHash());

    soft.assertThat(diff(fromRef.getName(), toRef.getHash(), Detached.REF_NAME, toRefHash))
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // both nameless references
    soft.assertThat(diff(Detached.REF_NAME, fromRef.getHash(), Detached.REF_NAME, toRefHash))
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // reverse to/from
    soft.assertThat(diff(Detached.REF_NAME, toRefHash, fromRef.getName(), fromRef.getHash()))
        .hasSize(1)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNull();
            });
  }

  @Test
  public void diff() throws BaseNessieClientServerException {
    Reference fromRef = createBranch("diffFrom", treeApi().getDefaultBranch());
    Reference toRef = createBranch("diffTo", treeApi().getDefaultBranch());
    String toRefHash = createCommits(toRef, 1, 1, toRef.getHash());
    toRef = Branch.of(toRef.getName(), toRefHash);

    List<DiffEntry> diffs = diff(fromRef, toRef);

    assertThat(diffs).hasSize(1);
  }
}
