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
import org.projectnessie.model.Reference;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateNamespaceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableCreateReferenceCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableShowDiffCommandSpec;
import org.projectnessie.nessie.cli.cmdspec.ImmutableUseReferenceCommandSpec;

public class TestShowDiff extends BaseTestCommand {
  @Test
  public void diffBetweenTwoReferences() throws Exception {
    try (NessieCliTester cli = nessieCliTester()) {
      Reference main = cli.getCurrentReference();

      cli.execute(
          ImmutableCreateReferenceCommandSpec.of(
              null, null, "BRANCH", "diff-branch", main.getName(), main.getHash(), false));
      cli.execute(ImmutableUseReferenceCommandSpec.of(null, null, "BRANCH", "diff-branch", null));

      // Only visible on 'diff-branch', not on 'main'.
      cli.execute(ImmutableCreateNamespaceCommandSpec.of(null, null, "foo", null, Map.of()));

      List<String> diff =
          cli.execute(
              ImmutableShowDiffCommandSpec.of(
                  null, null, main.getName(), null, null, "diff-branch", null));

      soft.assertThat(diff)
          .anySatisfy(
              line ->
                  soft.assertThat(line)
                      .contains("diff --nessie")
                      .contains("a/" + main.getName())
                      .contains("b/diff-branch"))
          .anySatisfy(line -> soft.assertThat(line).startsWith("A").contains("foo"));

      // Diffing a reference against itself yields no content differences, only the header.
      List<String> noDiff =
          cli.execute(
              ImmutableShowDiffCommandSpec.of(
                  null, null, "diff-branch", null, null, "diff-branch", null));
      soft.assertThat(noDiff).noneMatch(line -> line.startsWith("A") || line.startsWith("D"));

      // 'FROM' defaults to the CLI's current reference ('diff-branch' after USE REFERENCE above).
      List<String> defaultFrom =
          cli.execute(ImmutableShowDiffCommandSpec.of(null, null, null, null, null, "main", null));
      soft.assertThat(defaultFrom)
          .anySatisfy(
              line ->
                  soft.assertThat(line)
                      .contains("a/diff-branch")
                      .contains("b/" + main.getName()));
    }
  }
}
