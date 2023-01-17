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

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.params.FetchOption;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Reference;

class ITDeleteContent extends AbstractContentGeneratorTest {

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV1 api = buildNessieApi()) {
      branch = makeCommit(api);
    }
  }

  @Test
  void deleteContent() throws NessieNotFoundException {

    ProcessResult proc =
        runGeneratorCmd(
            "delete",
            "--uri",
            NESSIE_API_URI,
            "--branch",
            branch.getName(),
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc.getExitCode()).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    Reference head;
    try (NessieApiV1 api = buildNessieApi()) {
      head = api.getReference().refName(branch.getName()).fetch(FetchOption.ALL).get();

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains(CONTENT_KEY.toString());
    }

    assertThat(output).anySatisfy(s -> assertThat(s).contains(head.getHash()));
  }

  @Test
  void deleteContentWithMessage() throws NessieNotFoundException {

    ProcessResult proc =
        runGeneratorCmd(
            "delete",
            "--uri",
            NESSIE_API_URI,
            "--message",
            "test-message-123",
            "--branch",
            branch.getName(),
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc.getExitCode()).isEqualTo(0);

    try (NessieApiV1 api = buildNessieApi()) {
      CommitMeta commitMeta =
          api.getCommitLog().refName(branch.getName()).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("test-message-123");
    }
  }
}
