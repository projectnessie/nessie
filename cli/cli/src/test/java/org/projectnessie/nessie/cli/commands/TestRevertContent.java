/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.nessie.cli.commands;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.CommitResponse;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cli.CliCommandFailedException;
import org.projectnessie.nessie.cli.cmdspec.ImmutableListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableRevertContentCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ListContentsCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.RevertContentCommandSpec;

public class TestRevertContent extends BaseTestCommand {
  @Test
  public void revertContent() throws Exception {
    try (NessieCliTester cli = nessieCliTester()) {
      Branch beginning = (Branch) cli.getCurrentReference();

      ContentKey t1 = ContentKey.of("t1");
      ContentKey t2 = ContentKey.of("t2");
      ContentKey t3 = ContentKey.of("t3");
      IcebergTable t1c1 = IcebergTable.of("t1c1", 1, 2, 3, 4);
      IcebergTable t1c2 = IcebergTable.of("t1c2", 5, 6, 7, 8);
      IcebergTable t2c2 = IcebergTable.of("t2c2", 42, 6, 7, 8);
      IcebergTable t3c1 = IcebergTable.of("t3c1", 66, 6, 7, 8);

      CommitResponse c1 =
          cli.mandatoryNessieApi()
              .commitMultipleOperations()
              .branch(beginning)
              .operation(Operation.Put.of(t1, t1c1))
              .operation(Operation.Put.of(t3, t3c1))
              .commitMeta(CommitMeta.fromMessage("c1"))
              .commitWithResponse();

      t1c1 = t1c1.withId(c1.toAddedContentsMap().get(t1));
      t1c2 = t1c2.withId(c1.toAddedContentsMap().get(t1));
      t3c1 = t3c1.withId(c1.toAddedContentsMap().get(t3));

      CommitResponse c2 =
          cli.mandatoryNessieApi()
              .commitMultipleOperations()
              .branch(c1.getTargetBranch())
              .operation(Operation.Put.of(t1, t1c2))
              .operation(Operation.Put.of(t2, t2c2))
              .commitMeta(CommitMeta.fromMessage("c2"))
              .commitWithResponse();

      t2c2 = t2c2.withId(c2.toAddedContentsMap().get(t2));

      ListContentsCommandSpec listContents =
          ImmutableListContentsCommandSpec.of(null, null, null, null, null, null, null);
      soft.assertThat(cli.execute(listContents))
          .containsExactly("  ICEBERG_TABLE t1", "  ICEBERG_TABLE t2", "  ICEBERG_TABLE t3");

      // A dummy commit to have a different "from" hash than c2

      CommitResponse c3 =
          cli.mandatoryNessieApi()
              .commitMultipleOperations()
              .branch(c1.getTargetBranch())
              .operation(Operation.Put.of(ContentKey.of("dummy"), Namespace.of("dummy")))
              .commitMeta(CommitMeta.fromMessage("dummy"))
              .commitWithResponse();

      cli.setCurrentReference(c3.getTargetBranch());

      // DRY run of REVERT CONTENT where all keys exist (no ALLOW DELETES)

      RevertContentCommandSpec revertContentDry1 =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c2.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .isDryRun(true)
              .build();
      List<String> executedDry1 = cli.execute(revertContentDry1);
      soft.assertThat(executedDry1)
          .contains(
              "Reverted content keys t1, t2, t3 to state on branch main at "
                  + c2.getTargetBranch().getHash()
                  + " from hash "
                  + c3.getTargetBranch().getHash()
                  + " :")
          .contains("  Key t1 updated from")
          .contains("  Key t2 updated from")
          .contains("  Key t3 updated from")
          .contains("Dry run, not committing any changes.");

      // REVERT CONTENT where all keys exist (no ALLOW DELETES)

      RevertContentCommandSpec revertContent1 =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c2.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .build();
      List<String> executed1 = cli.execute(revertContent1);
      soft.assertThat(executed1)
          .contains(
              "Reverted content keys t1, t2, t3 to state on branch main at "
                  + c2.getTargetBranch().getHash()
                  + " from hash "
                  + c3.getTargetBranch().getHash()
                  + " :")
          .contains("  Key t1 updated from")
          .contains("  Key t2 updated from")
          .contains("  Key t3 updated from")
          .anyMatch(s -> s.startsWith("Target branch main is now at commit "));

      soft.assertThat(
              cli.mandatoryNessieApi()
                  .getContent()
                  .keys(List.of(t1, t2, t3))
                  .refName(beginning.getName())
                  .get())
          .containsAllEntriesOf(Map.of(t1, t1c2, t2, t2c2, t3, t3c1));

      // Must fail without ALLOW DELETES, also for DRY

      RevertContentCommandSpec revertContent2 =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c1.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .build();
      soft.assertThatThrownBy(() -> cli.execute(revertContent2))
          .isInstanceOf(CliCommandFailedException.class);
      soft.assertThat(cli.capturedOutput())
          .contains(
              "Content key "
                  + t2.toPathString()
                  + " does not exist and ALLOW DELETES clause was not specified.");

      RevertContentCommandSpec revertContent2a =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c1.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .isDryRun(true)
              .build();
      soft.assertThatThrownBy(() -> cli.execute(revertContent2a))
          .isInstanceOf(CliCommandFailedException.class);
      soft.assertThat(cli.capturedOutput())
          .contains(
              "Content key "
                  + t2.toPathString()
                  + " does not exist and ALLOW DELETES clause was not specified.");

      Reference head = cli.mandatoryNessieApi().getReference().refName("main").get();

      // DRY run with ALLOW DELETES

      RevertContentCommandSpec revertContentDry3 =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c1.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .isDryRun(true)
              .isAllowDeletes(true)
              .build();
      List<String> executedDry3 = cli.execute(revertContentDry3);
      soft.assertThat(executedDry3)
          .contains(
              "Reverted content keys t1, t2, t3 to state on branch main at "
                  + c1.getTargetBranch().getHash()
                  + " from hash "
                  + head.getHash()
                  + " :")
          .contains("  Key t1 updated from")
          .contains("  Key t2 was deleted")
          .contains("  Key t3 updated from")
          .contains("Dry run, not committing any changes.");

      // With ALLOW DELETES

      RevertContentCommandSpec revertContent3 =
          ImmutableRevertContentCommandSpec.builder()
              .sourceRefTimestampOrHash(c1.getTargetBranch().getHash())
              .contentKeys(List.of(t1.toPathString(), t2.toPathString(), t3.toPathString()))
              .isAllowDeletes(true)
              .build();
      List<String> executed3 = cli.execute(revertContent3);
      soft.assertThat(executed3)
          .contains(
              "Reverted content keys t1, t2, t3 to state on branch main at "
                  + c1.getTargetBranch().getHash()
                  + " from hash "
                  + head.getHash()
                  + " :")
          .contains("  Key t1 updated from")
          .contains("  Key t2 was deleted")
          .contains("  Key t3 updated from")
          .anyMatch(s -> s.startsWith("Target branch main is now at commit "));

      soft.assertThat(
              cli.mandatoryNessieApi()
                  .getContent()
                  .keys(List.of(t1, t2, t3))
                  .refName(beginning.getName())
                  .get())
          .containsAllEntriesOf(Map.of(t1, t1c1, t3, t3c1))
          .doesNotContainKey(t2);
    }
  }
}
