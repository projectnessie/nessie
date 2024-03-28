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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.ITReadCommits.assertOutputContains;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITDeleteRecursive extends AbstractContentGeneratorTest {

  protected static final ContentKey KEY1 = ContentKey.of("first", "t1");
  protected static final ContentKey KEY2 = ContentKey.of("first", "nested", "t2");
  protected static final ContentKey KEY3 = ContentKey.of("other", "t3");

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
      String branchName = "test-" + UUID.randomUUID();
      Branch main = api.getDefaultBranch();
      branch =
          (Branch)
              api.createReference()
                  .sourceRefName(main.getName())
                  .reference(Branch.of(branchName, main.getHash()))
                  .create();

      IcebergTable table = IcebergTable.of("testMeta", 1, 2, 3, 4);
      branch =
          api.commitMultipleOperations()
              .branchName(branch.getName())
              .hash(branch.getHash())
              .commitMeta(CommitMeta.fromMessage(COMMIT_MSG))
              .operation(Operation.Put.of(KEY1, table))
              .operation(Operation.Put.of(KEY2.getNamespace().toContentKey(), KEY2.getNamespace()))
              .operation(Operation.Put.of(KEY2, table))
              .operation(Operation.Put.of(KEY3.getNamespace().toContentKey(), KEY3.getNamespace()))
              .operation(Operation.Put.of(KEY3, table))
              .commit();
    }
  }

  @Test
  void deleteRecursive() throws NessieNotFoundException {
    ProcessResult proc =
        runGeneratorCmd(
            "delete-recursive",
            "--uri",
            NESSIE_API_URI,
            "--verbose",
            "--branch",
            branch.getName(),
            "--key",
            KEY1.getNamespace().getElements().get(0));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    Reference head;
    try (NessieApiV2 api = buildNessieApi()) {
      head = api.getReference().refName(branch.getName()).get();

      assertThat(api.getEntries().reference(head).stream())
          .map(EntriesResponse.Entry::getName)
          .containsExactlyInAnyOrder(KEY3, KEY3.getNamespace().toContentKey());

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("Recursively deleted 4 entries at first");
    }

    List<String> output = proc.getStdOutLines();
    assertOutputContains(
        output,
        "Fetching existing entries...",
        "Deleting 4 entries...",
        "Deleted 4 entries at " + head);
  }

  @Test
  void deleteEmptyNamespaceOnMain() throws NessieNotFoundException {
    ProcessResult proc =
        runGeneratorCmd(
            "delete-recursive",
            "--uri",
            NESSIE_API_URI,
            "--key",
            CONTENT_KEY.getNamespace().getElements().get(0));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    Reference head;
    try (NessieApiV2 api = buildNessieApi()) {
      head = api.getDefaultBranch();

      assertThat(api.getEntries().reference(head).stream()).isEmpty();

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("Recursively deleted 1 entries at first");
    }

    List<String> output = proc.getStdOutLines();
    assertOutputContains(output, "Deleted 1 entries at " + head);
  }

  @Test
  void deleteNothing() throws NessieNotFoundException {
    Branch main;
    try (NessieApiV2 api = buildNessieApi()) {
      main = api.getDefaultBranch();
    }

    ProcessResult proc =
        runGeneratorCmd("delete-recursive", "--uri", NESSIE_API_URI, "--key", "non-existent");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    try (NessieApiV2 api = buildNessieApi()) {
      assertThat(api.getDefaultBranch()).isEqualTo(main); // no changes committed
    }

    List<String> output = proc.getStdOutLines();
    assertOutputContains(output, "Nothing to delete.");
  }

  @Test
  void deleteTableFails() {
    ProcessResult proc =
        runGeneratorCmd(
            "delete-recursive",
            "--uri",
            NESSIE_API_URI,
            "--branch",
            branch.getName(),
            "--key",
            KEY1.getElements().get(0),
            "--key",
            KEY1.getElements().get(1));

    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(1);

    assertOutputContains(
        proc.getStdErrLines(),
        "Key first.t1 does not point to a Namespace, it points to ICEBERG_TABLE");
  }

  @Test
  void deleteOnTagFails() throws NessieNotFoundException, NessieConflictException {
    Reference tag;
    try (NessieApiV2 api = buildNessieApi()) {
      Branch main = api.getDefaultBranch();
      tag =
          api.createReference()
              .reference(Tag.of("test-tag", main.getHash()))
              .sourceRefName(main.getName())
              .create();
    }

    ProcessResult proc =
        runGeneratorCmd(
            "delete-recursive",
            "--uri",
            NESSIE_API_URI,
            "--ref",
            tag.getName(),
            "--key",
            KEY1.getElements().get(0));

    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(1);

    assertOutputContains(proc.getStdErrLines(), "Reference test-tag is not a branch.");
  }
}
