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
package org.projectnessie.jaxrs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Delete;
import org.projectnessie.model.Reference;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestDiff extends AbstractRestContents {
  @Test
  public void testDiff() throws BaseNessieClientServerException {
    int commitsPerBranch = 10;

    Reference fromRef =
        getApi().createReference().reference(Branch.of("testDiffFromRef", null)).create();
    Reference toRef =
        getApi().createReference().reference(Branch.of("testDiffToRef", null)).create();
    String toRefHash = createCommits(toRef, 1, commitsPerBranch, toRef.getHash());

    // we only committed to toRef, the "from" diff should be null
    assertThat(
            getApi()
                .getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNull();
              assertThat(diff.getTo()).isNotNull();
            });

    // after committing to fromRef, "from/to" diffs should both have data
    createCommits(fromRef, 1, commitsPerBranch, fromRef.getHash());

    assertThat(
            getApi()
                .getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
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

    List<ContentKey> keys =
        IntStream.rangeClosed(0, commitsPerBranch)
            .mapToObj(i -> ContentKey.of("table" + i))
            .collect(Collectors.toList());
    // request all keys and delete the tables for them on toRef
    Map<ContentKey, Content> map = getApi().getContent().refName(toRef.getName()).keys(keys).get();
    for (Map.Entry<ContentKey, Content> entry : map.entrySet()) {
      toRef =
          getApi()
              .commitMultipleOperations()
              .branchName(toRef.getName())
              .hash(toRefHash)
              .commitMeta(CommitMeta.fromMessage("delete"))
              .operation(Delete.of(entry.getKey()))
              .commit();
    }

    // now that we deleted all tables on toRef, the diff for "to" should be null
    assertThat(
            getApi()
                .getDiff()
                .fromRefName(fromRef.getName())
                .toRefName(toRef.getName())
                .get()
                .getDiffs())
        .hasSize(commitsPerBranch)
        .allSatisfy(
            diff -> {
              assertThat(diff.getKey()).isNotNull();
              assertThat(diff.getFrom()).isNotNull();
              assertThat(diff.getTo()).isNull();
            });
  }
}
