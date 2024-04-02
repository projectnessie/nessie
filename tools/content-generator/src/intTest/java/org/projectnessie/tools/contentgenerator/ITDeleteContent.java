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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;

class ITDeleteContent extends AbstractContentGeneratorTest {

  private Branch branch;

  @BeforeEach
  void setup() throws NessieConflictException, NessieNotFoundException {
    try (NessieApiV2 api = buildNessieApi()) {
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
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    Reference head;
    try (NessieApiV2 api = buildNessieApi()) {
      head = api.getReference().refName(branch.getName()).fetch(FetchOption.ALL).get();

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains(CONTENT_KEY.toString());
    }

    assertThat(output).anySatisfy(s -> assertThat(s).contains(head.getHash()));
  }

  @Test
  void deleteAllContent() throws NessieNotFoundException {
    ProcessResult proc = runGeneratorCmd("delete", "--uri", NESSIE_API_URI, "--all");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    Reference head;
    try (NessieApiV2 api = buildNessieApi()) {
      head = api.getReference().refName(branch.getName()).fetch(FetchOption.ALL).get();

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("Delete 2 keys");

      assertThat(api.getEntries().refName(branch.getName()).get().getEntries()).isEmpty();
      assertThat(api.getEntries().refName("main").get().getEntries()).isEmpty();
    }

    assertThat(output).anySatisfy(s -> assertThat(s).contains(head.getHash()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"|", ""})
  void deleteFromFile(String separator, @TempDir File tempDir) throws IOException {
    File input = new File(tempDir, "delete-keys.csv");
    writeInputFile(input, separator, CONTENT_KEY);

    List<String> args = new ArrayList<>();
    args.add("delete");
    args.add("--uri");
    args.add(NESSIE_API_URI);
    args.add("--ref");
    args.add(branch.getName());
    args.add("--input");
    args.add(input.getAbsolutePath());
    args.add("--format=CSV_KEYS");
    if (!separator.isEmpty()) {
      args.add("--separator");
      args.add(separator);
    }
    ProcessResult proc = runGeneratorCmd(args.toArray(new String[0]));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);
    List<String> output = proc.getStdOutLines();

    Reference head;
    try (NessieApiV2 api = buildNessieApi()) {
      head = api.getReference().refName(branch.getName()).fetch(FetchOption.ALL).get();

      CommitMeta commitMeta =
          api.getCommitLog().reference(head).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("Delete " + CONTENT_KEY);

      assertThat(api.getEntries().refName(branch.getName()).get().getEntries())
          .map(EntriesResponse.Entry::getName)
          .doesNotContain(CONTENT_KEY);
    }

    assertThat(output).anySatisfy(s -> assertThat(s).contains(head.getHash()));
  }

  private void writeInputFile(File file, String separator, ContentKey... keys) throws IOException {
    try (PrintWriter w = new PrintWriter(file)) {
      for (ContentKey key : keys) {
        if (separator == null || separator.isEmpty()) {
          w.println(key.toPathString());
        } else {
          w.println(String.join(separator, key.getElements()));
        }
      }
    }
  }

  @Test
  void deleteContentWithMessage() throws NessieNotFoundException {

    ProcessResult proc =
        runGeneratorCmd(
            "delete",
            "--uri",
            NESSIE_API_URI,
            "--author",
            "Test Author 123 <test@example.com>",
            "--message",
            "test-message-123",
            "--branch",
            branch.getName(),
            "--key",
            CONTENT_KEY.getElements().get(0),
            "--key",
            CONTENT_KEY.getElements().get(1));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    try (NessieApiV2 api = buildNessieApi()) {
      CommitMeta commitMeta =
          api.getCommitLog().refName(branch.getName()).get().getLogEntries().get(0).getCommitMeta();
      assertThat(commitMeta.getMessage()).contains("test-message-123");
      assertThat(commitMeta.getAuthor()).isEqualTo("Test Author 123 <test@example.com>");
    }
  }

  @Test
  void deleteRecursive() throws NessieNotFoundException, NessieConflictException {
    try (NessieApiV2 api = buildNessieApi()) {
      IcebergTable table = IcebergTable.of("testMeta", 1, 2, 3, 4);
      Namespace nested = Namespace.of(CONTENT_KEY.getElements().get(0), "nested");
      branch =
          api.commitMultipleOperations()
              .branchName(branch.getName())
              .hash(branch.getHash())
              .commitMeta(CommitMeta.fromMessage(COMMIT_MSG))
              .operation(Operation.Put.of(nested.toContentKey(), nested))
              .operation(Operation.Put.of(ContentKey.of(nested, "t1"), table))
              .operation(Operation.Put.of(ContentKey.of(nested, "t2"), table))
              .commit();
    }

    ProcessResult proc =
        runGeneratorCmd(
            "delete",
            "--uri",
            NESSIE_API_URI,
            "--batch",
            "1", // validate deleting keys before namespaces
            "--branch",
            branch.getName(),
            "--recursive",
            "--key",
            CONTENT_KEY.getElements().get(0));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    try (NessieApiV2 api = buildNessieApi()) {
      assertThat(api.getEntries().refName(branch.getName()).stream()).isEmpty();
    }
  }

  @Test
  void deleteAllRecursive() throws NessieNotFoundException {

    ProcessResult proc =
        runGeneratorCmd(
            "delete",
            "--uri",
            NESSIE_API_URI,
            "--all",
            "--recursive",
            "--key",
            CONTENT_KEY.getElements().get(0));
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    try (NessieApiV2 api = buildNessieApi()) {
      assertThat(api.getEntries().refName(branch.getName()).stream()).isEmpty();
      assertThat(api.getEntries().reference(api.getDefaultBranch()).stream()).isEmpty();
    }
  }

  @Test
  void deleteNothing() throws NessieNotFoundException {
    Branch main;
    try (NessieApiV2 api = buildNessieApi()) {
      main = api.getDefaultBranch();
    }

    ProcessResult proc =
        runGeneratorCmd("delete", "--uri", NESSIE_API_URI, "--recursive", "--key", "non-existent");
    assertThat(proc).extracting(ProcessResult::getExitCode).isEqualTo(0);

    try (NessieApiV2 api = buildNessieApi()) {
      assertThat(api.getDefaultBranch()).isEqualTo(main); // no changes committed
    }
  }
}
