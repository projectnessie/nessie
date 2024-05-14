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
package org.projectnessie.tools.contentgenerator.cli;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.jaxrs.ext.NessieJaxRsExtension.jaxRsExtension;
import static org.projectnessie.model.CommitMeta.fromMessage;
import static org.projectnessie.model.Content.Type.NAMESPACE;
import static org.projectnessie.tools.contentgenerator.RunContentGenerator.runGeneratorCmd;
import static org.projectnessie.tools.contentgenerator.cli.CreateMissingNamespaces.branchesStream;
import static org.projectnessie.tools.contentgenerator.cli.CreateMissingNamespaces.collectMissingNamespaceKeys;

import java.net.URI;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.ext.NessieClientFactory;
import org.projectnessie.client.ext.NessieClientUri;
import org.projectnessie.jaxrs.ext.NessieJaxRsExtension;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.EntriesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Namespace;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.tools.contentgenerator.RunContentGenerator.ProcessResult;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.inmemorytests.InmemoryBackendTestFactory;
import org.projectnessie.versioned.storage.testextension.NessieBackend;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.NessieStoreConfig;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
@NessieBackend(InmemoryBackendTestFactory.class)
public class TestCreateMissingNamespaces {
  @InjectSoftAssertions protected SoftAssertions soft;

  @NessiePersist
  @NessieStoreConfig(name = "namespace-validation", value = "false")
  static Persist persist;

  @RegisterExtension static NessieJaxRsExtension server = jaxRsExtension(() -> persist);

  private NessieApiV2 nessieApi;
  private URI uri;
  private final CreateMissingNamespaces cmd = new CreateMissingNamespaces();

  @BeforeEach
  public void setUp(NessieClientFactory clientFactory, @NessieClientUri URI uri) {
    nessieApi = (NessieApiV2) clientFactory.make();
    this.uri = uri;
  }

  @AfterEach
  public void tearDown() {
    nessieApi.close();
  }

  @Test
  public void roundtrip() throws Exception {
    prepareRoundtrip();

    ProcessResult result =
        runGeneratorCmd(
            "create-missing-namespaces",
            "--author",
            "Author 123",
            "--verbose",
            "--uri",
            uri.toString());

    soft.assertThat(result)
        .extracting(
            ProcessResult::getExitCode,
            ProcessResult::getStdOutLines,
            ProcessResult::getStdErrLines)
        .containsExactly(
            0,
            asList(
                "Start fetching and processing references...",
                "  processing branch branch1...",
                "    creating 3 namespaces...",
                "      - a",
                "      - a.b",
                "      - x",
                "    ... committed.",
                "  processing branch branch2...",
                "    creating 2 namespaces...",
                "      - x",
                "      - x.y",
                "    ... committed.",
                "  processing branch branch3...",
                "    all namespaces present.",
                "  processing branch main...",
                "    all namespaces present.",
                "Successfully processed 4 branches, created 5 namespaces."),
            singletonList(""));

    soft.assertThat(nessieApi.getCommitLog().refName("branch1").stream())
        .isNotEmpty()
        .first(type(LogResponse.LogEntry.class))
        .extracting(LogResponse.LogEntry::getCommitMeta)
        .extracting(CommitMeta::getAuthor)
        .isEqualTo("Author 123");
  }

  @Test
  public void roundtripNonExistingBranch() throws Exception {
    prepareRoundtrip();

    ProcessResult result =
        runGeneratorCmd(
            "create-missing-namespaces", "--verbose", "--uri", uri.toString(), "--branch", "foo");
    soft.assertThat(result)
        .extracting(
            ProcessResult::getExitCode,
            ProcessResult::getStdOutLines,
            ProcessResult::getStdErrLines)
        .containsExactly(
            1,
            asList(
                "Start fetching and processing references...",
                "Successfully processed 0 branches, created 0 namespaces."),
            asList(
                "Could not find branch(es) foo specified as command line arguments.",
                "See above messages for errors!"));
  }

  @Test
  public void roundtripSomeBranches() throws Exception {
    prepareRoundtrip();

    ProcessResult result =
        runGeneratorCmd(
            "create-missing-namespaces",
            "--verbose",
            "--uri",
            uri.toString(),
            "--branch",
            "branch1",
            "--branch",
            "branch3");

    soft.assertThat(result)
        .extracting(
            ProcessResult::getExitCode,
            ProcessResult::getStdOutLines,
            ProcessResult::getStdErrLines)
        .containsExactly(
            0,
            asList(
                "Start fetching and processing references...",
                "  processing branch branch1...",
                "    creating 3 namespaces...",
                "      - a",
                "      - a.b",
                "      - x",
                "    ... committed.",
                "  processing branch branch3...",
                "    all namespaces present.",
                "Successfully processed 2 branches, created 3 namespaces."),
            singletonList(""));
  }

  protected void prepareRoundtrip() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();
    Branch branch1 =
        (Branch)
            nessieApi
                .createReference()
                .sourceRefName(defaultBranch.getName())
                .reference(Branch.of("branch1", defaultBranch.getHash()))
                .create();
    Branch branch2 =
        (Branch)
            nessieApi
                .createReference()
                .sourceRefName(defaultBranch.getName())
                .reference(Branch.of("branch2", defaultBranch.getHash()))
                .create();
    Branch branch3 =
        (Branch)
            nessieApi
                .createReference()
                .sourceRefName(defaultBranch.getName())
                .reference(Branch.of("branch3", defaultBranch.getHash()))
                .create();

    nessieApi
        .commitMultipleOperations()
        .commitMeta(fromMessage("foo"))
        .branch(branch1)
        .operation(Put.of(ContentKey.of("a", "b", "Table"), IcebergTable.of("meta1", 1, 2, 3, 4)))
        .operation(Put.of(ContentKey.of("a", "b", "Data"), IcebergTable.of("meta2", 1, 2, 3, 4)))
        .operation(Put.of(ContentKey.of("Data"), IcebergTable.of("meta3", 1, 2, 3, 4)))
        .operation(Put.of(ContentKey.of("x", "Data"), IcebergTable.of("meta3", 1, 2, 3, 4)))
        .commit();

    nessieApi
        .commitMultipleOperations()
        .commitMeta(fromMessage("foo"))
        .branch(branch2)
        .operation(Put.of(ContentKey.of("x", "y", "Table"), IcebergTable.of("meta1", 1, 2, 3, 4)))
        .commit();

    nessieApi
        .commitMultipleOperations()
        .commitMeta(fromMessage("foo"))
        .branch(branch3)
        .operation(Put.of(ContentKey.of("Table"), IcebergTable.of("meta1", 1, 2, 3, 4)))
        .commit();
  }

  @Test
  public void testBranchesStream() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();
    nessieApi
        .createReference()
        .sourceRefName(defaultBranch.getName())
        .reference(Branch.of("branch1", defaultBranch.getHash()))
        .create();
    nessieApi
        .createReference()
        .sourceRefName(defaultBranch.getName())
        .reference(Branch.of("branch2", defaultBranch.getHash()))
        .create();
    nessieApi
        .createReference()
        .sourceRefName(defaultBranch.getName())
        .reference(Branch.of("branch3", defaultBranch.getHash()))
        .create();

    soft.assertThat(branchesStream(nessieApi, n -> true))
        .map(Branch::getName)
        .containsExactlyInAnyOrder("branch1", "branch2", "branch3", defaultBranch.getName());

    soft.assertThat(branchesStream(nessieApi, n -> n.equals("branch2") || n.equals("branch3")))
        .map(Branch::getName)
        .containsExactlyInAnyOrder("branch2", "branch3");

    soft.assertThat(branchesStream(nessieApi, n -> n.equals("branch2")))
        .map(Branch::getName)
        .containsExactlyInAnyOrder("branch2");
  }

  @Test
  public void testCollectMissingNamespaceKeys() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();
    Branch branch =
        (Branch)
            nessieApi
                .createReference()
                .sourceRefName(defaultBranch.getName())
                .reference(Branch.of("branch", defaultBranch.getHash()))
                .create();

    Branch head =
        nessieApi
            .commitMultipleOperations()
            .commitMeta(fromMessage("foo"))
            .branch(branch)
            .operation(
                Put.of(ContentKey.of("a", "b", "Table"), IcebergTable.of("meta1", 1, 2, 3, 4)))
            .operation(
                Put.of(ContentKey.of("a", "b", "Data"), IcebergTable.of("meta2", 1, 2, 3, 4)))
            .operation(Put.of(ContentKey.of("Data"), IcebergTable.of("meta3", 1, 2, 3, 4)))
            .operation(Put.of(ContentKey.of("x", "Data"), IcebergTable.of("meta3", 1, 2, 3, 4)))
            .commit();

    soft.assertThat(collectMissingNamespaceKeys(nessieApi, head))
        .containsExactlyInAnyOrder(ContentKey.of("a"), ContentKey.of("a", "b"), ContentKey.of("x"));

    head =
        nessieApi
            .commitMultipleOperations()
            .branch(head)
            .commitMeta(fromMessage("a"))
            .operation(Put.of(ContentKey.of("a"), Namespace.of("a")))
            .commit();

    soft.assertThat(collectMissingNamespaceKeys(nessieApi, head))
        .containsExactlyInAnyOrder(ContentKey.of("a", "b"), ContentKey.of("x"));

    head =
        nessieApi
            .commitMultipleOperations()
            .branch(head)
            .commitMeta(fromMessage("a.b"))
            .operation(Put.of(ContentKey.of("a", "b"), Namespace.of("a", "b")))
            .commit();

    soft.assertThat(collectMissingNamespaceKeys(nessieApi, head))
        .containsExactly(ContentKey.of("x"));

    head =
        nessieApi
            .commitMultipleOperations()
            .branch(head)
            .commitMeta(fromMessage("x"))
            .operation(Put.of(ContentKey.of("x"), Namespace.of("x")))
            .commit();

    soft.assertThat(collectMissingNamespaceKeys(nessieApi, head)).isEmpty();
  }

  @Test
  public void testCommitCreateNamespaces() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();

    cmd.commitCreateNamespaces(
        nessieApi,
        defaultBranch,
        asList(
            ContentKey.of("a"),
            ContentKey.of("a", "b"),
            ContentKey.of("x"),
            ContentKey.of("x", "y")));

    soft.assertThat(nessieApi.getEntries().refName(defaultBranch.getName()).stream())
        .extracting(EntriesResponse.Entry::getName, EntriesResponse.Entry::getType)
        .containsExactlyInAnyOrder(
            tuple(ContentKey.of("a"), NAMESPACE),
            tuple(ContentKey.of("a", "b"), NAMESPACE),
            tuple(ContentKey.of("x"), NAMESPACE),
            tuple(ContentKey.of("x", "y"), NAMESPACE));
  }
}
