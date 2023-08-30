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
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITReadCommits extends AbstractContentGeneratorTest {

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
      branch = makeCommit(api);
    }
  }

  @Test
  void readCommits() throws Exception {
    ProcessResult proc =
        runGeneratorCmd("commits", "--uri", NESSIE_API_URI, "--ref", branch.getName());

    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    assertThat(output).anySatisfy(s -> assertThat(s).contains(COMMIT_MSG));
    assertThat(output).noneSatisfy(s -> assertThat(s).contains(CONTENT_KEY.toString()));

    try (NessieApiV2 api = buildNessieApi()) {
      assertThat(api.getCommitLog().refName(branch.getName()).stream()).hasSize(2);
    }
  }

  @Test
  void readCommitsFullHashes() {
    ProcessResult proc =
        runGeneratorCmd(
            "commits", "--uri", NESSIE_API_URI, "--ref", branch.getName(), "--full-hashes");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();
    assertThat(output).anySatisfy(s -> assertThat(s).matches("[0-9a-f]{32}.*" + COMMIT_MSG + ".*"));
  }

  @Test
  void readCommitsVerbose() throws Exception {
    ProcessResult proc =
        runGeneratorCmd("commits", "--uri", NESSIE_API_URI, "--ref", branch.getName(), "--verbose");

    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    assertThat(output).anySatisfy(s -> assertThat(s).contains(COMMIT_MSG));
    assertThat(output).anySatisfy(s -> assertThat(s).contains(CONTENT_KEY.toString()));
    assertThat(output)
        .anySatisfy(s -> assertThat(s).contains("key[0]: " + CONTENT_KEY.getElements().get(0)));
    assertThat(output)
        .anySatisfy(s -> assertThat(s).contains("key[1]: " + CONTENT_KEY.getElements().get(1)));

    try (NessieApiV2 api = buildNessieApi()) {
      List<LogEntry> logEntries =
          api.getCommitLog().refName(branch.getName()).fetch(FetchOption.ALL).stream()
              .collect(Collectors.toList());
      assertThat(logEntries).hasSize(2);
      assertThat(logEntries.get(0).getOperations()).isNotEmpty();
      assertThat(logEntries.get(0).getParentCommitHash()).isNotNull();
    }
  }

  @ParameterizedTest
  @CsvSource(
      value = {"%1$s|false", "%2$s|true", "%2$s~1|false"},
      delimiter = '|')
  void readCommitsWithHash(String hash, boolean expect2Commits) throws Exception {

    String c1 = branch.getHash();
    try (NessieApiV2 api = buildNessieApi()) {
      branch =
          api.commitMultipleOperations()
              .branchName(branch.getName())
              .hash(c1)
              .commitMeta(CommitMeta.fromMessage("Second commit"))
              .operation(Operation.Delete.of(CONTENT_KEY))
              .commit();
    }
    String c2 = branch.getHash();

    ProcessResult proc =
        runGeneratorCmd(
            "commits",
            "--uri",
            NESSIE_API_URI,
            "--ref",
            branch.getName(),
            "--hash",
            String.format(hash, c1, c2));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();
    assertThat(output).anySatisfy(s -> assertThat(s).contains(COMMIT_MSG));
    if (expect2Commits) {
      assertThat(output).anySatisfy(s -> assertThat(s).contains("Second commit"));
    } else {
      assertThat(output).noneSatisfy(s -> assertThat(s).contains("Second commit"));
    }
  }
}
