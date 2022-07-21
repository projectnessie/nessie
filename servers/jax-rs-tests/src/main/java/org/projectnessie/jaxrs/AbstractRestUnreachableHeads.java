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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Tag;

/** See {@link AbstractTestRest} for details about and reason for the inheritance model. */
public abstract class AbstractRestUnreachableHeads extends AbstractRestNamespace {
  @Test
  public void testUnreachableHeads() throws BaseNessieClientServerException {
    String tagName = "tag1_test_unreachable_heads";
    String branchName1 = "branch1_test_unreachable_heads";
    String branchName2 = "branch2_test_unreachable_heads";

    //                    /----(tag1)
    //  ---(branch1) --- commit1
    //                   \-----(branch2) -- commit2

    // 1: creating the default branch1
    Branch branch1 = createBranch(branchName1);

    // 2: commit on default branch1
    IcebergTable meta = IcebergTable.of("meep", 42, 42, 42, 42);
    branch1 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch1.getName())
            .hash(branch1.getHash())
            .commitMeta(
                CommitMeta.builder()
                    .message("dummy commit log")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentKey.of("meep"), meta))
            .commit();

    // 3: create tag1
    Tag createdTag =
        (Tag)
            getApi()
                .createReference()
                .sourceRefName(branch1.getName())
                .reference(Tag.of(tagName, branch1.getHash()))
                .create();

    // 4. drop tag1
    getApi().deleteTag().tag(createdTag).delete();

    // no unreferenced head as the tag's head is still referenced by branch1
    assertThat(getApi().getUnreachableReferenceHeads().contains(branch1.getHash())).isFalse();

    // 5. create branch2
    Branch branch2 = createBranch(branchName2, branch1);

    // 6: commit on default branch2
    meta = IcebergTable.of("meep", 43, 42, 42, 42);
    branch2 =
        getApi()
            .commitMultipleOperations()
            .branchName(branch2.getName())
            .hash(branch2.getHash())
            .commitMeta(
                CommitMeta.builder()
                    .message("dummy commit log")
                    .properties(ImmutableMap.of("prop1", "val1", "prop2", "val2"))
                    .build())
            .operation(Operation.Put.of(ContentKey.of("meep"), meta))
            .commit();

    // 7. assign branch1 to branch2.
    getApi().assignBranch().branch(branch2).assignTo(branch1).assign();

    // branch2's old head becomes unreferenced due to assign operation.
    assertThat(getApi().getUnreachableReferenceHeads().contains(branch2.getHash())).isTrue();

    // 8. delete branch2
    getApi().deleteBranch().branchName(branch2.getName()).hash(branch1.getHash()).delete();
    // branch1's head should not be there as branch1 is not dropped.
    assertThat(getApi().getUnreachableReferenceHeads())
        .contains(branch2.getHash())
        .doesNotContain(branch1.getHash());

    // 8. delete branch1
    getApi().deleteBranch().branch(branch1).delete();
    // still should not contain branch1's head, as it was never global head.
    assertThat(getApi().getUnreachableReferenceHeads())
        .contains(branch2.getHash())
        .doesNotContain(branch1.getHash());
  }
}
