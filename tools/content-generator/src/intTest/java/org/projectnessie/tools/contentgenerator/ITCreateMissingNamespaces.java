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
package org.projectnessie.tools.contentgenerator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

public class ITCreateMissingNamespaces extends AbstractContentGeneratorTest {

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
      branch = makeCommit(api);
    }
  }

  @Test
  void nothingToDo() {
    ProcessResult proc =
        runGeneratorCmd("create-missing-namespaces", "--verbose", "--uri", NESSIE_API_URI);
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    assertThat(proc.getStdOutLines())
        .contains("Start fetching and processing references...")
        .contains("  processing branch main...")
        .contains("    all namespaces present.")
        .contains("  processing branch " + branch.getName() + "...")
        .contains("Successfully processed 2 branches, created 0 namespaces.");
    assertThat(proc.getStdErrLines()).containsExactly("");
  }

  @Test
  void nothingToDoExplicitBranches() {
    ProcessResult proc =
        runGeneratorCmd(
            "create-missing-namespaces",
            "--verbose",
            "--uri",
            NESSIE_API_URI,
            "--branch",
            "main",
            "--branch",
            branch.getName());
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    assertThat(proc.getStdOutLines())
        .contains("Start fetching and processing references...")
        .contains("  processing branch main...")
        .contains("    all namespaces present.")
        .contains("  processing branch " + branch.getName() + "...")
        .contains("Successfully processed 2 branches, created 0 namespaces.");
    assertThat(proc.getStdErrLines()).containsExactly("");
  }

  @Test
  void nothingToDoNonExistingBranches() {
    ProcessResult proc =
        runGeneratorCmd(
            "create-missing-namespaces",
            "--verbose",
            "--uri",
            NESSIE_API_URI,
            "--branch",
            "not-there",
            "--branch",
            "not-there-as-well");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(1);

    assertThat(proc.getStdOutLines())
        .contains("Start fetching and processing references...")
        .doesNotContain("  processing branch main...")
        .doesNotContain("  processing branch " + branch.getName() + "...")
        .contains("Successfully processed 0 branches, created 0 namespaces.");
    assertThat(proc.getStdErrLines())
        .contains(
            "Could not find branch(es) not-there, not-there-as-well specified as command line arguments.")
        .contains("See above messages for errors!");
  }
}
