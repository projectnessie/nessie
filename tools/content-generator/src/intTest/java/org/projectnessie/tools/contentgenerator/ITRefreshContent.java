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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.Tag;

public class ITRefreshContent extends AbstractContentGeneratorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final ContentKey key1 = ContentKey.of("test", "key1");
  private static final ContentKey key2 = ContentKey.of("test", "key2");
  private static final ContentKey key3 = ContentKey.of("test_key3");
  private static final IcebergTable table1 = IcebergTable.of("meta_111", 1, 2, 3, 4);
  private static final IcebergTable table2 = IcebergTable.of("meta_222", 1, 2, 3, 4);
  private static final IcebergTable table3 = IcebergTable.of("meta_333", 1, 2, 3, 4);

  private NessieApiV2 api;

  @BeforeEach
  void setUp() {
    api = buildNessieApi();
  }

  private void create(IcebergTable table, ContentKey key)
      throws NessieConflictException, NessieNotFoundException {
    if (key.getElementCount() > 1) {
      try {
        api.createNamespace()
            .namespace(key.getNamespace())
            .reference(api.getDefaultBranch())
            .create();
      } catch (NessieNamespaceAlreadyExistsException ignore) {
        //
      }
    }
    api.commitMultipleOperations()
        .branch(api.getDefaultBranch())
        .commitMeta(CommitMeta.fromMessage("test-commit"))
        .operation(Operation.Put.of(key, table))
        .commit();
  }

  private IcebergTable get(ContentKey key) throws NessieNotFoundException {
    return get("main", key);
  }

  private IcebergTable get(String refName, ContentKey key) throws NessieNotFoundException {
    return (IcebergTable) api.getContent().refName(refName).key(key).get().get(key);
  }

  private List<LogResponse.LogEntry> log(int depth) throws NessieNotFoundException {
    return log(depth, api.getDefaultBranch());
  }

  private List<LogResponse.LogEntry> log(int depth, Reference ref) throws NessieNotFoundException {
    return api.getCommitLog().reference(ref).fetch(FetchOption.ALL).stream()
        .limit(depth)
        .collect(Collectors.toList());
  }

  private int runMain(String... args) {
    List<String> allArgs = new ArrayList<>();
    allArgs.add("content-refresh");
    allArgs.add("--uri");
    allArgs.add(NESSIE_API_URI);
    allArgs.addAll(Arrays.asList(args));

    return runGeneratorCmd(allArgs.toArray(new String[0])).getExitCode();
  }

  @Test
  void refreshOneKey() throws NessieNotFoundException, NessieConflictException {
    create(table1, key1);
    IcebergTable stored1 = get(key1);

    assertThat(
            runMain(
                "--author",
                "Test Author 123",
                "--key",
                key1.getElements().get(0),
                "--key",
                key1.getElements().get(1),
                "--ref",
                api.getDefaultBranch().getName()))
        .isEqualTo(0);

    assertThat(get(key1)).isEqualTo(stored1);
    assertThat(log(1))
        .first()
        .extracting(
            logEntry -> logEntry.getCommitMeta().getMessage(),
            logEntry -> logEntry.getCommitMeta().getAuthor(),
            logEntry -> Objects.requireNonNull(logEntry.getOperations()).get(0).getKey())
        .containsExactly("Refresh 1 key(s)", "Test Author 123", key1);
  }

  @Test
  void refreshEmptyBatch() throws NessieNotFoundException {
    assertThat(runMain("--key", "unknown_key", "--ref", api.getDefaultBranch().getName()))
        .isEqualTo(0);
  }

  @Test
  void failOnExplicitTagArgument() throws NessieNotFoundException, NessieConflictException {
    create(table3, key3);
    IcebergTable stored3 = get(key3);

    String tagName = "testTag_" + UUID.randomUUID();
    Branch main = api.getDefaultBranch();
    Reference tag =
        api.createReference()
            .reference(Tag.of(tagName, main.getHash()))
            .sourceRefName(main.getName())
            .create();

    LogResponse.LogEntry head = log(1, tag).get(0);

    assertThat(runMain("--key", key3.toString(), "--ref", tagName)).isEqualTo(1);

    assertThat(log(1, tag).get(0)).isEqualTo(head); // no new commits
    assertThat(get(key3)).isEqualTo(stored3);
  }

  private void writeInputFile(File file, ContentKey... keys) throws IOException {
    String ref = api.getDefaultBranch().getName();
    MAPPER.writeValue(
        file,
        Arrays.stream(keys)
            .map(
                k ->
                    ImmutableMap.of(
                        "reference", ref, "key", ImmutableMap.of("elements", k.getElements())))
            .collect(Collectors.toList()));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 100})
  void refreshFromFile(int batchSize, @TempDir File tempDir) throws IOException {
    create(table1, key1);
    create(table2, key2);
    create(table3, key3);
    IcebergTable stored1 = get(key1);
    IcebergTable stored2 = get(key2);
    IcebergTable stored3 = get(key3);

    File input = new File(tempDir, "refresh-keys.json");
    writeInputFile(input, key1, key2, key3);

    assertThat(
            runMain(
                "--input",
                input.getAbsolutePath(),
                "--format=CONTENT_INFO_JSON",
                "--batch",
                String.valueOf(batchSize),
                "--message",
                "Test refresh message"))
        .isEqualTo(0);

    assertThat(get(key1)).isEqualTo(stored1);
    assertThat(get(key2)).isEqualTo(stored2);
    assertThat(get(key3)).isEqualTo(stored3);

    int numRefreshCommits = (3 / batchSize) + (3 % batchSize > 0 ? 1 : 0);

    assertThat(log(numRefreshCommits))
        .allSatisfy(
            logEntry ->
                assertThat(logEntry.getCommitMeta().getMessage()).isEqualTo("Test refresh message"))
        .flatExtracting(
            logEntry ->
                Objects.requireNonNull(logEntry.getOperations()).stream()
                    .map(Operation::getKey)
                    .collect(Collectors.toList()))
        .containsExactlyInAnyOrder(key1, key2, key3);
  }

  @Test
  void partialRefreshFromFileWithRefOveride(@TempDir File tempDir) throws IOException {
    create(table1, key1);
    IcebergTable stored1 = get(key1);

    Branch main = api.getDefaultBranch();
    Reference ref = api.createReference().reference(Branch.of("test", main.getHash())).create();

    File input = new File(tempDir, "refresh-keys.json");
    writeInputFile(input, key1, key2);

    assertThat(
            runMain(
                "--input",
                input.getAbsolutePath(),
                "--format=CONTENT_INFO_JSON",
                "--branch",
                ref.getName(),
                "--message",
                "Test refresh message"))
        .isEqualTo(0);

    assertThat(api.getDefaultBranch()).isEqualTo(main);

    assertThat(log(1, api.getReference().refName(ref.getName()).get()))
        .allSatisfy(
            logEntry ->
                assertThat(logEntry.getCommitMeta().getMessage()).isEqualTo("Test refresh message"))
        .flatExtracting(
            logEntry ->
                Objects.requireNonNull(logEntry.getOperations()).stream()
                    .map(Operation::getKey)
                    .collect(Collectors.toList()))
        .containsExactly(key1); // Note: key2 is not present

    assertThat(get(ref.getName(), key1)).isEqualTo(stored1);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 4, 5, 6, 100})
  void refreshAllKeys(int batchSize) throws IOException {
    create(table1, key1);
    create(table2, key2);
    create(table3, key3);
    IcebergTable stored1 = get(key1);
    IcebergTable stored2 = get(key2);
    IcebergTable stored3 = get(key3);

    assertThat(
            runMain(
                "--all", "--batch", String.valueOf(batchSize), "--message", "Test refresh message"))
        .isEqualTo(0);

    assertThat(get(key1)).isEqualTo(stored1);
    assertThat(get(key2)).isEqualTo(stored2);
    assertThat(get(key3)).isEqualTo(stored3);

    int numEntries =
        3 + 1 + 1; // 3 test keys + the "first" namespace from the superclass + the "test" namespace
    int numRefreshCommits = (numEntries / batchSize) + (numEntries % batchSize > 0 ? 1 : 0);

    assertThat(log(numRefreshCommits))
        .allSatisfy(
            logEntry ->
                assertThat(logEntry.getCommitMeta().getMessage()).isEqualTo("Test refresh message"))
        .flatExtracting(
            logEntry ->
                Objects.requireNonNull(logEntry.getOperations()).stream()
                    .map(Operation::getKey)
                    .collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            key1,
            key2,
            key3,
            key1.getNamespace().toContentKey(),
            CONTENT_KEY.getNamespace().toContentKey());
  }
}
