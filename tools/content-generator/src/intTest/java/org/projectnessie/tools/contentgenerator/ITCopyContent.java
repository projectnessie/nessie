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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITCopyContent extends AbstractContentGeneratorTest {

  @ParameterizedTest
  @ValueSource(strings = {"", "test1"})
  void basicCopyContent(String branchName) throws Exception {
    int numCommits = 20;

    try (NessieApiV2 api = buildNessieApi()) {

      Branch branch = api.getDefaultBranch();
      if (!branchName.isEmpty()) {
        Branch main = api.getDefaultBranch();
        branch =
            (Branch)
                api.createReference()
                    .sourceRefName(main.getName())
                    .reference(Branch.of(branchName, main.getHash()))
                    .create();
      }

      ContentKey src = ContentKey.of("test", "key");
      branch =
          api.commitMultipleOperations()
              .branch(branch)
              .commitMeta(CommitMeta.fromMessage("test initial"))
              .operation(Operation.Put.of(src.getNamespace().toContentKey(), src.getNamespace()))
              .operation(Operation.Put.of(src, IcebergTable.of("loc", 1, 2, 3, 4)))
              .commit();

      ProcessResult proc =
          runGeneratorCmd(
              "copy",
              "--author",
              "Test Author ABC",
              "-n",
              Integer.toString(numCommits),
              "-u",
              NESSIE_API_URI,
              "--ref",
              branch.getName(),
              "--from",
              "test",
              "--from",
              "key",
              "--to",
              "test",
              "--to",
              "copy-%d");

      assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

      Set<String> expectedKeys = new HashSet<>();
      expectedKeys.add(src.getNamespace().toCanonicalString());
      expectedKeys.add(src.toCanonicalString());
      expectedKeys.add(CONTENT_KEY.getNamespace().toCanonicalString());
      for (int i = 0; i < numCommits; i++) {
        expectedKeys.add("test.copy-" + i);
      }

      assertThat(
              api.getEntries().refName(branch.getName()).stream()
                  .map(EntriesResponse.Entry::getName)
                  .map(ContentKey::toCanonicalString)
                  .collect(Collectors.toSet()))
          .containsExactlyInAnyOrderElementsOf(expectedKeys);
    }
  }
}
