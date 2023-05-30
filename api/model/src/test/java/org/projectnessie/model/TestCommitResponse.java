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
package org.projectnessie.model;

import static org.projectnessie.model.CommitResponse.AddedContent.addedContent;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCommitResponse {
  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void addedContentKey() {
    Branch branch = Branch.of("branch", "11223344");
    IcebergTable table = IcebergTable.of("x", 1, 2, 3, 4);

    // REST API v1
    soft.assertThatThrownBy(
            () ->
                CommitResponse.builder()
                    .targetBranch(branch)
                    .build()
                    .contentWithAssignedId(ContentKey.of("foo"), table))
        .isInstanceOf(UnsupportedOperationException.class);

    // REST API v2 cases
    soft.assertThat(
            CommitResponse.builder()
                .addAddedContents(addedContent(ContentKey.of("bar"), "1234"))
                .targetBranch(branch)
                .build()
                .contentWithAssignedId(ContentKey.of("foo"), table))
        .isSameAs(table);
    soft.assertThat(
            CommitResponse.builder()
                .addAddedContents(addedContent(ContentKey.of("foo"), "1234"))
                .targetBranch(branch)
                .build()
                .contentWithAssignedId(ContentKey.of("foo"), table))
        .isEqualTo(table.withId("1234"));
  }
}
