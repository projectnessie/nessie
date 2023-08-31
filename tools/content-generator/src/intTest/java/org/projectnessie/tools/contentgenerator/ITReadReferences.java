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
import static org.projectnessie.tools.contentgenerator.ITReadCommits.assertOutputDoesNotContain;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITReadReferences extends AbstractContentGeneratorTest {

  private Branch branch1;
  private Branch branch2;
  private Branch branch3;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
      branch1 = makeCommit(api);
      branch2 = makeCommit(api);
      branch3 = makeCommit(api);
    }
  }

  @Test
  void readReferences() {
    ProcessResult proc = runGeneratorCmd("refs", "--uri", NESSIE_API_URI);
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();
    assertOutputContains(
        output,
        "Reading all references...",
        "main",
        branch1.getName(),
        branch2.getName(),
        branch3.getName(),
        "Done reading 4 references.");
  }

  @Test
  void readReferencesLimit() {
    ProcessResult proc = runGeneratorCmd("refs", "--uri", NESSIE_API_URI, "--limit", "1");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();
    assertOutputContains(
        output, "Reading up to 1 references...", "main", "Done reading 1 references.");
    assertOutputDoesNotContain(output, branch1.getName(), branch2.getName(), branch3.getName());
  }
}
